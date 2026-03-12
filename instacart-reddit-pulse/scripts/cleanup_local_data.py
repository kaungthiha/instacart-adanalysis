from __future__ import annotations

import shutil
from pathlib import Path

TARGETS = [
    Path("data/reddit_pulse.db"),
    Path("cache"),
    Path("data/raw"),
    Path("outputs/raw_text"),
]


def delete_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


if __name__ == "__main__":
    for target in TARGETS:
        delete_path(target)
    print("Cleanup complete. Local cached/raw data removed.")
