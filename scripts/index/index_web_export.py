#!/usr/bin/env python3
"""Index `web-search` exports through the generic Apertus metadata-aware indexer."""


import subprocess
import sys
from pathlib import Path


# Prevent callers from overriding the dataset flags this wrapper is meant to fix.
def _reject_flag(flag_name: str) -> None:
    for argument in sys.argv[1:]:
        if argument == flag_name or argument.startswith(f"{flag_name}="):
            raise SystemExit(f"{flag_name} is fixed by this wrapper and should not be provided explicitly.")


# Delegate to the existing generic indexer while pinning the web-specific flags.
def main() -> None:
    _reject_flag("--dataset-type")
    _reject_flag("--metadata-fields")

    target_script = Path(__file__).resolve().with_name("index_with_metadata.py")
    command = [
        sys.executable,
        str(target_script),
        "--dataset-type",
        "web",
        "--metadata-fields",
        "web",
        *sys.argv[1:],
    ]
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
