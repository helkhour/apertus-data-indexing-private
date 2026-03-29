# Writes canonical web ingest records and audit rows to parquet or JSONL outputs.
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Sequence

import pandas as pd


# Writes one ordered parquet file using the provided canonical columns.
def write_parquet(records: Iterable[Dict], output_path: Path, columns: Sequence[str]) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for record in records:
        # Preserve the canonical column order so parquet exports stay predictable
        # for the metadata-aware indexer and for downstream schema inspection.
        rows.append({column: record.get(column) for column in columns})
    dataframe = pd.DataFrame(rows, columns=list(columns))
    dataframe.to_parquet(output_path, index=False)


# Writes one JSONL file when parquet is not the desired audit format.
def write_jsonl(records: Iterable[Dict], output_path: Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
