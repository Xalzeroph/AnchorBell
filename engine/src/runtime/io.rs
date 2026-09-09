use std::{
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, sync::mpsc, task::JoinHandle};

const LINE_WRITER_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const INITIAL_ZSTD_LEVEL: i32 = 3;
const DEEP_ZSTD_LEVEL: i32 = 9;
const DEEP_RECOMPRESS_MIN_SAVINGS_BPS: u64 = 100;

pub struct AsyncLineWriter {
    pub sender: mpsc::Sender<String>,
    pub task: JoinHandle<Result<u64, io::Error>>,
    pub written: Arc<AtomicU64>,
    pub dropped: Arc<AtomicU64>,
}

pub async fn send_line(
    sender: &mpsc::Sender<String>,
    dropped: &Arc<AtomicU64>,
    line: String,
) -> Result<(), mpsc::error::SendError<String>> {
    match sender.send(line).await {
        Ok(()) => Ok(()),
        Err(error) => {
            dropped.fetch_add(1, Ordering::Relaxed);
            Err(error)
        }
    }
}

pub async fn spawn_line_writer(
    path: Option<PathBuf>,
    channel_capacity: usize,
    buffer_capacity: usize,
    flush_every: u32,
) -> AsyncLineWriter {
    let (sender, mut receiver) = mpsc::channel::<String>(channel_capacity.max(1));
    let written = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let written_count = Arc::clone(&written);
    let task = tokio::spawn(async move {
        let Some(path) = path else {
            while receiver.recv().await.is_some() {
                written_count.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(written_count.load(Ordering::Relaxed));
        };
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| io_context("create line-writer directory", parent, error))?;
        }
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(|error| io_context("open line-writer file", &path, error))?;
        let mut writer = tokio::io::BufWriter::with_capacity(buffer_capacity.max(1), file);
        let mut pending = 0_u32;
        let mut segment = 0_u64;
        let mut segment_bytes = 0_u64;
        let mut flush_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        flush_tick.tick().await;
        loop {
            tokio::select! {
                line = receiver.recv() => {
                    let Some(line) = line else { break };
                    let line_bytes = line.len() as u64 + 1;
                    if segment_bytes > 0
                        && segment_bytes.saturating_add(line_bytes) > LINE_WRITER_SEGMENT_BYTES
                    {
                        writer
                            .flush()
                            .await
                            .map_err(|error| io_context("rotate line-writer file", &path, error))?;
                        drop(writer);
                        compress_segment(&path, segment).await?;
                        segment = segment.saturating_add(1);
                        let file = tokio::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&path)
                            .await
                            .map_err(|error| io_context("reopen line-writer file", &path, error))?;
                        writer = tokio::io::BufWriter::with_capacity(buffer_capacity.max(1), file);
                        segment_bytes = 0;
                    }
                    writer
                        .write_all(line.as_bytes())
                        .await
                        .map_err(|error| io_context("write line-writer record", &path, error))?;
                    writer
                        .write_all(b"\n")
                        .await
                        .map_err(|error| io_context("write line-writer newline", &path, error))?;
                    written_count.fetch_add(1, Ordering::Relaxed);
                    pending = pending.saturating_add(1);
                    segment_bytes = segment_bytes.saturating_add(line_bytes);
                    if pending >= flush_every.max(1) {
                        writer
                            .flush()
                            .await
                            .map_err(|error| io_context("flush line-writer file", &path, error))?;
                        pending = 0;
                    }
                }
                _ = flush_tick.tick(), if pending > 0 => {
                    writer
                        .flush()
                        .await
                        .map_err(|error| io_context("periodic flush line-writer file", &path, error))?;
                    pending = 0;
                }
            }
        }
        writer
            .flush()
            .await
            .map_err(|error| io_context("finalize line-writer file", &path, error))?;
        Ok(written_count.load(Ordering::Relaxed))
    });
    AsyncLineWriter {
        sender,
        task,
        written,
        dropped,
    }
}
async fn compress_segment(path: &Path, segment: u64) -> Result<(), io::Error> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::other("line-writer path has no UTF-8 file name"))?;
    let raw_path = path.with_file_name(format!("{file_name}.segment-{segment:06}"));
    let archive_path = raw_path.with_file_name(format!("{file_name}.segment-{segment:06}.zst"));
    let metadata_path =
        raw_path.with_file_name(format!("{file_name}.segment-{segment:06}.zst.meta.json"));
    tokio::fs::rename(path, &raw_path)
        .await
        .map_err(|error| io_context("archive line-writer segment", &raw_path, error))?;
    tokio::task::spawn_blocking(move || {
        compress_segment_blocking(raw_path, archive_path, metadata_path)
    })
    .await
    .map_err(|error| io::Error::other(format!("compression task failed: {error}")))?
}

fn compress_segment_blocking(
    raw_path: PathBuf,
    archive_path: PathBuf,
    metadata_path: PathBuf,
) -> Result<(), io::Error> {
    let mut input = File::open(&raw_path)
        .map_err(|error| io_context("open raw line-writer segment", &raw_path, error))?;
    let temporary_archive = archive_path.with_extension("zst.tmp");
    let archive = File::create(&temporary_archive).map_err(|error| {
        io_context(
            "create temporary compressed line-writer archive",
            &temporary_archive,
            error,
        )
    })?;
    let mut encoder = zstd::stream::write::Encoder::new(archive, INITIAL_ZSTD_LEVEL)
        .map_err(|error| io_context("create zstd encoder", &raw_path, error))?;
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut hasher = Sha256::new();
    let mut uncompressed_bytes = 0_u64;
    let mut line_count = 0_u64;
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|error| io_context("read raw line-writer segment", &raw_path, error))?;
        if read == 0 {
            break;
        }
        let bytes = &buffer[..read];
        encoder
            .write_all(bytes)
            .map_err(|error| io_context("write zstd line-writer archive", &archive_path, error))?;
        hasher.update(bytes);
        uncompressed_bytes = uncompressed_bytes.saturating_add(read as u64);
        line_count =
            line_count.saturating_add(bytes.iter().filter(|byte| **byte == b'\n').count() as u64);
    }
    let archive = encoder
        .finish()
        .map_err(|error| io_context("finish zstd line-writer archive", &archive_path, error))?;
    archive
        .sync_all()
        .map_err(|error| io_context("sync zstd line-writer archive", &archive_path, error))?;
    let compressed_bytes = archive
        .metadata()
        .map_err(|error| io_context("inspect zstd line-writer archive", &archive_path, error))?
        .len();
    if compressed_bytes == 0 {
        let _ = std::fs::remove_file(&temporary_archive);
        return Err(io_context(
            "verify zstd line-writer archive",
            &temporary_archive,
            io::Error::other("compressed archive is empty"),
        ));
    }
    std::fs::rename(&temporary_archive, &archive_path)
        .map_err(|error| io_context("publish zstd line-writer archive", &archive_path, error))?;
    let (compression_level, compressed_bytes) =
        maybe_deep_recompress(&archive_path, compressed_bytes)?;
    let metadata = serde_json::json!({
        "schema_version": 1,
        "compression": "zstd",
        "compression_level": compression_level,
        "compression_stage": if compression_level == DEEP_ZSTD_LEVEL { "deep" } else { "base" },
        "uncompressed_bytes": uncompressed_bytes,
        "compressed_bytes": compressed_bytes,
        "line_count": line_count,
        "sha256": hex::encode(hasher.finalize()),
    });
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)
        .map_err(|error| io::Error::other(error.to_string()))?;
    let temporary_metadata = metadata_path.with_extension("json.tmp");
    std::fs::write(&temporary_metadata, metadata_bytes)
        .map_err(|error| io_context("write zstd archive metadata", &temporary_metadata, error))?;
    std::fs::rename(&temporary_metadata, &metadata_path)
        .map_err(|error| io_context("publish zstd archive metadata", &metadata_path, error))?;
    std::fs::remove_file(&raw_path)
        .map_err(|error| io_context("remove verified raw line-writer segment", &raw_path, error))?;
    Ok(())
}
fn maybe_deep_recompress(path: &Path, current_bytes: u64) -> Result<(i32, u64), io::Error> {
    // A second pass is deliberately bounded: recompressing compressed bytes
    // indefinitely cannot improve entropy and can make archives larger.
    if current_bytes < 1024 {
        return Ok((INITIAL_ZSTD_LEVEL, current_bytes));
    }
    let temporary = path.with_extension("zst.deep.tmp");
    let input = File::open(path)
        .map_err(|error| io_context("open base zstd archive for deep pass", path, error))?;
    let mut decoder = zstd::stream::read::Decoder::new(input)
        .map_err(|error| io_context("create deep zstd decoder", path, error))?;
    let archive = File::create(&temporary)
        .map_err(|error| io_context("create deep zstd temporary archive", &temporary, error))?;
    let mut encoder = zstd::stream::write::Encoder::new(archive, DEEP_ZSTD_LEVEL)
        .map_err(|error| io_context("create deep zstd encoder", &temporary, error))?;
    io::copy(&mut decoder, &mut encoder)
        .map_err(|error| io_context("deep zstd recompression", path, error))?;
    let archive = encoder
        .finish()
        .map_err(|error| io_context("finish deep zstd archive", &temporary, error))?;
    archive
        .sync_all()
        .map_err(|error| io_context("sync deep zstd archive", &temporary, error))?;
    let deep_bytes = archive
        .metadata()
        .map_err(|error| io_context("inspect deep zstd archive", &temporary, error))?
        .len();
    let required_savings = 10_000_u64.saturating_sub(DEEP_RECOMPRESS_MIN_SAVINGS_BPS);
    if deep_bytes > 0
        && deep_bytes.saturating_mul(10_000) < current_bytes.saturating_mul(required_savings)
    {
        std::fs::rename(&temporary, path)
            .map_err(|error| io_context("publish deep zstd archive", path, error))?;
        Ok((DEEP_ZSTD_LEVEL, deep_bytes))
    } else {
        let _ = std::fs::remove_file(&temporary);
        Ok((INITIAL_ZSTD_LEVEL, current_bytes))
    }
}

pub async fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), io::Error> {
    let bytes = serde_json::to_vec(value).map_err(|error| io::Error::other(error.to_string()))?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| io_context("create atomic-write directory", parent, error))?;
    }
    // A unique temporary name prevents concurrent snapshots from colliding.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    let temporary = path.with_extension(format!("json.tmp.{}.{}", std::process::id(), nonce));
    tokio::fs::write(&temporary, bytes)
        .await
        .map_err(|error| io_context("write atomic temporary file", &temporary, error))?;
    replace_file(&temporary, path)?;
    Ok(())
}

fn io_context(operation: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} '{}': {error}", path.display()),
    )
}

fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };
        let source_wide = source
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let target_wide = target
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let mut last_error = None;
        for attempt in 0..5 {
            let result = unsafe {
                MoveFileExW(
                    source_wide.as_ptr(),
                    target_wide.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };
            if result != 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            last_error = Some(error);
            if attempt < 4 {
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        }
        Err(io_context(
            &format!(
                "replace atomic file '{}' -> '{}'",
                source.display(),
                target.display()
            ),
            target,
            last_error.unwrap_or_else(|| io::Error::other("unknown replace failure")),
        ))
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(source, target)
            .map_err(|error| io_context("replace atomic file", target, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_line_counts_closed_writer() {
        let (sender, receiver) = mpsc::channel::<String>(1);
        drop(receiver);
        let dropped = Arc::new(AtomicU64::new(0));
        assert!(send_line(&sender, &dropped, "lost".to_owned())
            .await
            .is_err());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn writer_counts_drained_lines_without_a_path() {
        let writer = spawn_line_writer(None, 2, 32, 1).await;
        writer.sender.send("one".to_owned()).await.unwrap();
        drop(writer.sender);
        assert_eq!(writer.task.await.unwrap().unwrap(), 1);
    }

    #[test]
    fn zstd_archive_round_trips_with_integrity_metadata() {
        let root = std::env::temp_dir().join(format!(
            "anchorbell-io-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let raw = root.join("records.jsonl.segment-000000");
        let archive = raw.with_file_name("records.jsonl.segment-000000.zst");
        let metadata = raw.with_file_name("records.jsonl.segment-000000.zst.meta.json");
        let original = b"{\"kind\":\"fill\",\"quantity\":3}\n{ \"kind\": \"cancel\" }\n";
        std::fs::write(&raw, original).unwrap();
        compress_segment_blocking(raw.clone(), archive.clone(), metadata.clone()).unwrap();
        let restored = zstd::stream::decode_all(File::open(&archive).unwrap()).unwrap();
        assert_eq!(restored, original);
        assert!(!raw.exists());
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&metadata).unwrap()).unwrap();
        assert_eq!(metadata["compression"], "zstd");
        assert_eq!(metadata["line_count"], 2);
        std::fs::remove_dir_all(root).unwrap();
    }
}
