"""Atomic JSON writes for the small persisted stores (categories, overrides, settings).

Each store writes to a temp file and renames it over the real one, so a crash halfway
through can never leave a truncated file behind.

The temp name must be UNIQUE PER WRITER. When every writer shared one `<file>.tmp`,
two concurrent writes raced: the first `os.replace` renamed the temp away and the second
died with `FileNotFoundError: ...json.tmp -> ...json`. That bit hardest on first run,
where the dashboard fires ~8 requests at once and each one tries to create the seed
catalog — several of them 500'd. `mkstemp` hands out a distinct name per call, so the
worst a concurrent write can do now is land second (last writer wins) instead of failing.
"""
import json
import os
import tempfile


def write_atomic(path, data):
    """Serialise `data` as JSON to `path`, atomically. Creates the directory if needed."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    # Same directory as the target, so the rename stays on one filesystem (and is atomic).
    fd, tmp = tempfile.mkstemp(dir=directory or '.',
                               prefix=os.path.basename(path) + '.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except BaseException:
        # Don't leave debris in the data dir if serialising or renaming failed.
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
