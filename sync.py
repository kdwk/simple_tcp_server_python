#!/usr/bin/python3

import shutil
import os
from pathlib import Path

def path(path: str) -> Path:
    return Path(__file__).parent / path

def folders() -> list[Path]:
    return [Path(filepath) for filepath in os.listdir(Path(__file__).parent) if Path(filepath).is_dir()]

def copy_and_replace(source_path, destination_path):
    if os.path.exists(destination_path):
        os.remove(destination_path)
    shutil.copy2(source_path, destination_path)

def sync():
    for folder in folders():
        if "src" not in folder.as_posix():
            copy_and_replace(path("src/client.py"), folder / "client.py")
            copy_and_replace(path("src/server.py"), folder / "server.py")

if __name__ == "__main__":
    sync()