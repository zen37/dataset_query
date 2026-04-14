#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path = [entry for entry in sys.path if Path(entry or ".").resolve() != SCRIPT_DIR]

from query_dataset.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["--engine", "adx", *sys.argv[1:]]))
