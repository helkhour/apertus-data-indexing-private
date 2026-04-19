#!/usr/bin/env python3
"""Run Apertus search in web mode so result rendering keeps URL provenance visible."""


import subprocess
import sys
from pathlib import Path


# Keep dataset selection owned by this wrapper so the generic search command is
# always invoked in the URL-aware rendering mode for web records.
def _reject_dataset_flag() -> None:
    for argument in sys.argv[1:]:
        if argument == "--dataset" or argument.startswith("--dataset="):
            raise SystemExit("--dataset is fixed by this wrapper and should not be provided explicitly.")


def main() -> None:
    _reject_dataset_flag()

    target_script = Path(__file__).resolve().with_name("search.py")
    command = [
        sys.executable,
        str(target_script),
        "--dataset",
        "web",
        *sys.argv[1:],
    ]
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
