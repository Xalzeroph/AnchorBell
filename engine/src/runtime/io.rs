use std::{
    io,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::{io::AsyncWriteExt, process::Command, sync::mpsc, task::JoinHandle};

const LINE_WRITER_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

pub struct AsyncLineWriter {
    pub sender: mpsc::Sender<String>,
    pub task: JoinHandle<Result<u64, io::Error>>,
    pub written: Arc<AtomicU64>,
    pub dropped: Arc<AtomicU64>,
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
    let archive_path = raw_path.with_extension("gz");
    tokio::fs::rename(path, &raw_path)
        .await
        .map_err(|error| io_context("archive line-writer segment", &raw_path, error))?;

    let raw_size = tokio::fs::metadata(&raw_path)
        .await
        .map_err(|error| io_context("inspect line-writer segment", &raw_path, error))?
        .len();
    if raw_size == 0 {
        tokio::fs::remove_file(&raw_path)
            .await
            .map_err(|error| io_context("discard empty line-writer segment", &raw_path, error))?;
        return Ok(());
    }

    let archive = std::fs::File::create(&archive_path).map_err(|error| {
        io_context(
            "create compressed line-writer archive",
            &archive_path,
            error,
        )
    })?;
    let output = Command::new("gzip")
        .arg("-n")
        .arg("-c")
        .arg(&raw_path)
        .stdout(Stdio::from(archive))
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|error| io_context("compress line-writer segment", &raw_path, error))?;
    if !output.status.success() {
        let _ = tokio::fs::remove_file(&archive_path).await;
        return Err(io_context(
            "compress line-writer segment",
            &raw_path,
            io::Error::other(String::from_utf8_lossy(&output.stderr).trim().to_owned()),
        ));
    }
    let archive_size = tokio::fs::metadata(&archive_path)
        .await
        .map_err(|error| {
            io_context(
                "verify compressed line-writer archive",
                &archive_path,
                error,
            )
        })?
        .len();
    if archive_size == 0 {
        let _ = tokio::fs::remove_file(&archive_path).await;
        return Err(io_context(
            "verify compressed line-writer archive",
            &archive_path,
            io::Error::other("compressed archive is empty"),
        ));
    }
    tokio::fs::remove_file(&raw_path)
        .await
        .map_err(|error| io_context("remove verified raw line-writer segment", &raw_path, error))?;
    Ok(())
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
    async fn writer_counts_drained_lines_without_a_path() {
        let writer = spawn_line_writer(None, 2, 32, 1).await;
        writer.sender.send("one".to_owned()).await.unwrap();
        drop(writer.sender);
        assert_eq!(writer.task.await.unwrap().unwrap(), 1);
    }
}
