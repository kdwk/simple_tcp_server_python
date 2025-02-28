#!/usr/bin/python3

from pathlib import Path
from sync import sync
from os.path import exists
from os import makedirs
import sys

def path(path: str) -> Path:
    return Path(__file__).parent / path

def newpeer():
    if len(sys.argv) < 2:
        print("newpeer must be called with name of new peer as argument")
    name = sys.argv[1]
    if not exists(path(name)):
        makedirs(path(name)/"served_files")
    else:
        print(f"Peer {name} already exists")
    sync()

if __name__ == "__main__":
    newpeer()