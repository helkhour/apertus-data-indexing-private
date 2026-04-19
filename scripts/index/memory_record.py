#!/usr/bin/env python3
"""
Canonical MemoryRecord builders for dataset and web artifact ingestion.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Optional, List


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _normalize_text(value: Any) -> str:
    text = _to_str(value).strip()
    if len(text) > 100000:
        return text[:100000] + "... [TRUNCATED]"
    return text


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_to_str(item) for item in value if item not in (None, "")]
    return [_to_str(value)] if value not in ("", None) else []


def _first_of(value: Any) -> Optional[str]:
    items = _normalize_list(value)
    if not items:
        return None
    return items[0]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_metadata_blob(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _build_agent(
    *,
    source_type: str,
    text: str,
    metadata: Dict[str, Any],
    trust: Dict[str, Any],
    record_type: str,
) -> Dict[str, Any]:
    text_len = len(text or "")
    can_quote = bool(trust.get("http_status") == 200 or source_type == "dataset")

    affordances = {
        "can_quote": can_quote,
        "can_ground_answer": text_len > 200,
        "can_summarize": text_len > 300,
        "can_execute": source_type == "code",
    }

    if record_type == "status":
        usage_hints = {
            "best_for": ["debugging", "audit"],
            "agent_hint": "use for provenance and trust checks",
        }
    elif source_type == "web":
        usage_hints = {
            "best_for": ["web grounding", "retrieval"],
            "agent_hint": "prefer quoting" if can_quote else "prefer summarizing",
        }
    else:
        usage_hints = {
            "best_for": ["retrieval"],
            "agent_hint": "prefer summarizing",
        }

    return {
        "metadata": metadata,
        "trust": trust,
        "affordances": affordances,
        "usage_hints": usage_hints,
    }


def build_dataset_memory_record(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    text = _normalize_text(row.get("text"))
    if not text:
        return None

    metadata_blob = _parse_metadata_blob(row.get("metadata"))
    metadata = {
        "url": metadata_blob.get("url") or row.get("url"),
        "title": metadata_blob.get("title") or row.get("title"),
        "lang": metadata_blob.get("lang") or row.get("lang"),
        "date": metadata_blob.get("date") or row.get("date"),
        "query": metadata_blob.get("query") or row.get("query"),
    }

    content_hash = _sha256(text)
    trust = {
        "robots_allowed": None,
        "http_status": None,
        "status": None,
        "error": None,
    }

    return {
        "record_id": content_hash,
        "parent_id": None,
        "record_type": "document",
        "source_type": "dataset",
        "search_text": text,
        "data": {
            "text": text,
            "table": None,
            "code": None,
            "graph": None,
        },
        "representations": {
            "summary": None,
            "embedding": None,
            "content_hash": content_hash,
            "chunk_refs": [],
            "content_hashes": [],
        },
        "agent": _build_agent(
            source_type="dataset",
            text=text,
            metadata=metadata,
            trust=trust,
            record_type="document",
        ),
    }


def infer_web_artifact_kind(
    row: Mapping[str, Any], artifact_hint: Optional[str] = None
) -> str:
    hint = (artifact_hint or "").lower()
    if "status" in hint:
        return "status"
    if "chunk" in hint:
        return "chunk"
    if "content_hashes" in row:
        return "chunk"
    if "status" in row and "content" not in row:
        return "status"
    if "content_hash" in row:
        return "content"
    return "content"


def _build_web_metadata(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "url": _normalize_list(row.get("url")),
        "path": _normalize_list(row.get("path")),
        "query": _normalize_list(row.get("query")),
        "title": _normalize_list(row.get("title")),
        "lang": _normalize_list(row.get("lang")),
        "date": _normalize_list(row.get("date")),
        "fetch_timestamp": row.get("fetch_timestamp"),
        "redirect_url": row.get("redirect_url"),
    }


def _build_web_trust(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "robots_allowed": row.get("robots_allowed"),
        "http_status": row.get("http_status"),
        "status": row.get("status"),
        "error": row.get("error"),
    }


def build_web_memory_record(
    row: Mapping[str, Any], artifact_hint: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    kind = infer_web_artifact_kind(row, artifact_hint=artifact_hint)
    text = _normalize_text(row.get("content") or row.get("text"))
    metadata = _build_web_metadata(row)
    trust = _build_web_trust(row)

    if kind == "status":
        status = _to_str(row.get("status"))
        key = "|".join(
            [
                _to_str(_first_of(row.get("path"))),
                status,
                _to_str(row.get("fetch_timestamp")),
                _to_str(_first_of(row.get("query"))),
            ]
        )
        record_id = _sha256(key)
        return {
            "record_id": record_id,
            "parent_id": None,
            "record_type": "status",
            "source_type": "web",
            "search_text": "",
            "data": {
                "text": None,
                "table": None,
                "code": None,
                "graph": None,
            },
            "representations": {
                "summary": None,
                "embedding": None,
                "content_hash": record_id,
                "chunk_refs": [],
                "content_hashes": [],
            },
            "agent": _build_agent(
                source_type="web",
                text="",
                metadata=metadata,
                trust=trust,
                record_type="status",
            ),
        }

    if not text:
        return None

    if kind == "chunk":
        content_hashes = _normalize_list(row.get("content_hashes"))
        record_id = _to_str(row.get("document_id")) or _sha256(text)
        parent_id = _first_of(content_hashes)
        content_hash = record_id
        record_type = "chunk"
    else:
        content_hash = _to_str(row.get("content_hash")) or _sha256(text)
        record_id = content_hash
        parent_id = None
        record_type = "document"
        content_hashes = []

    return {
        "record_id": record_id,
        "parent_id": parent_id,
        "record_type": record_type,
        "source_type": "web",
        "search_text": text,
        "data": {
            "text": text,
            "table": None,
            "code": None,
            "graph": None,
        },
        "representations": {
            "summary": None,
            "embedding": None,
            "content_hash": content_hash,
            "chunk_refs": _normalize_list(row.get("chunk_hashes")),
            "content_hashes": content_hashes,
        },
        "agent": _build_agent(
            source_type="web",
            text=text,
            metadata=metadata,
            trust=trust,
            record_type=record_type,
        ),
    }


def memory_record_to_es_action(record: Dict[str, Any], index_name: str) -> Dict[str, Any]:
    return {
        "_index": index_name,
        "_id": record["record_id"],
        "_source": record,
    }

