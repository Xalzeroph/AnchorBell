"""Conservative retention for sealed simulation market/evidence archives."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

ARCHIVE = re.compile(r"(?:shared-market|evidence-opportunities)\.jsonl\.segment-\d{6,}\.zst")
RUN = re.compile(r"simulation-.+-run-\d+")


def regular(path):
    return stat.S_ISREG(path.lstat().st_mode)


def sync_directory(path):
    if os.name == "posix":
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def write_json(path, value):
    temporary = path.with_name(path.name + f".tmp.{os.getpid()}")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def maintain(root, *, apply=False, max_bytes=8 * 1024**3,
             min_age_hours=24, max_age_days=7, free_floor=8 * 1024**3,
             now=None):
    root = Path(root).resolve(strict=True)
    if min(max_bytes, min_age_hours, max_age_days, free_floor) < 0:
        raise ValueError("retention limits must be nonnegative")
    now = time.time() if now is None else now
    archives = []
    total = 0
    for run in root.iterdir():
        if not RUN.fullmatch(run.name) or run.is_symlink() or not run.is_dir():
            continue
        manifest = run / "run-manifest.json"
        if not manifest.exists() or not regular(manifest):
            continue
        for path in run.iterdir():
            if not ARCHIVE.fullmatch(path.name) or not regular(path):
                continue
            info = path.stat()
            total += info.st_size
            metadata = Path(str(path) + ".meta.json")
            try:
                if not regular(metadata):
                    continue
                meta = json.loads(metadata.read_text(encoding="utf-8"))
                if (meta.get("schema_version") != 1 or meta.get("compression") != "zstd"
                        or meta.get("compressed_bytes") != info.st_size
                        or not re.fullmatch(r"[a-f0-9]{64}", meta.get("sha256", ""))):
                    continue
            except (OSError, ValueError, TypeError, AttributeError):
                continue
            archives.append((info.st_mtime, str(path), info.st_size, info.st_ino))
    available = shutil.disk_usage(root).free
    result = {"observed_at_ms": int(now * 1000), "apply": apply,
              "removed": [], "planned": [], "errors": [],
              "archive_bytes_before": total, "available_bytes_before": available}
    for modified, name, size, inode in sorted(archives):
        age = now - modified
        if age < min_age_hours * 3600:
            continue
        if total <= max_bytes and available >= free_floor and age < max_age_days * 86400:
            continue
        path = Path(name)
        if apply:
            try:
                # Recheck the exact closed file; never follow links or recurse.
                info = path.lstat()
                if (not stat.S_ISREG(info.st_mode) or path.parent.is_symlink()
                        or path.parent.resolve().parent != root
                        or (info.st_ino, info.st_size, info.st_mtime) != (inode, size, modified)):
                    raise ValueError("archive changed after planning")
                entry = {"at_ms": int(now * 1000), "path": str(path.relative_to(root)),
                         "bytes": size, "action": "delete_planned"}
                # Persist an intent and a partial-history marker BEFORE removal.
                # A crash can leave a conservative marker, never silent data loss.
                with (root / "retention-audit.jsonl").open("a", encoding="utf-8") as audit:
                    audit.write(json.dumps(entry) + "\n")
                    audit.flush()
                    os.fsync(audit.fileno())
                    sync_directory(root)
                    write_json(path.parent / "retention-status.json", {
                        "raw_archives_pruned": True,
                        "raw_replay_complete": False,
                        "trade_records_preserved": True,
                        "last_pruned_at_ms": int(now * 1000),
                        "audit_path": "../retention-audit.jsonl",
                    })
                    path.unlink()
                    sync_directory(path.parent)
                    entry["action"] = "deleted"
                    audit.write(json.dumps(entry) + "\n")
                    audit.flush()
                    os.fsync(audit.fileno())
                result["removed"].append(entry["path"])
            except (OSError, ValueError) as error:
                result["errors"].append(f"{path.name}: {error}")
                continue
        result["planned"].append(str(path.relative_to(root)))
        total -= size
        available = shutil.disk_usage(root).free if apply else available + size
    result.update(archive_bytes_after=total, available_bytes_after=available,
                  status="pressure" if total > max_bytes or available < free_floor else "healthy")
    if result["errors"]:
        result["status"] = "error"
    if apply:
        write_json(root / "retention-status.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="default is read-only planning")
    parser.add_argument("--max-gib", type=float, default=8)
    parser.add_argument("--min-age-hours", type=float, default=24)
    parser.add_argument("--max-age-days", type=float, default=7)
    parser.add_argument("--free-floor-gib", type=float, default=8)
    args = parser.parse_args()
    # systemd serializes timer invocations; flock also excludes manual CLI runs.
    lock = None
    if args.apply:
        import fcntl
        lock = (args.root / ".retention.lock").open("a")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    result = maintain(args.root, apply=args.apply, max_bytes=int(args.max_gib * 1024**3),
                      min_age_hours=args.min_age_hours, max_age_days=args.max_age_days,
                      free_floor=int(args.free_floor_gib * 1024**3))
    print(json.dumps(result))
    if lock:
        lock.close()
    return 0 if result["status"] == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
