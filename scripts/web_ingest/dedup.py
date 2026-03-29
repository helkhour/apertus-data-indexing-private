# Aggregates per-URL fetch results into shared content records and optional chunk records.
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

if __package__:
    from .schema import WEB_INDEX_COLUMNS, build_content_hash, canonicalize_record, ordered_unique
else:
    from schema import WEB_INDEX_COLUMNS, build_content_hash, canonicalize_record, ordered_unique


# Normalizes loose list-like values into flat string lists.
def _list_values(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)]


# Returns the first populated value seen across a group of per-URL records.
def _first_value(records: List[Dict[str, Any]], key: str) -> Any:
    for record in records:
        value = record.get(key)
        if value not in (None, "", []):
            return value
    return None


# Chunks text deterministically so repeated passages across pages can share one chunk record.
def chunk_text(text: str, *, target_words: int = 120, min_words: int = 40) -> List[str]:
    normalized = (text or "").strip()
    if not normalized:
        return []

    normalized = normalized.replace("\r\n", "\n")
    raw_segments = re.split(r"\n\s*\n+|(?<=[.!?])\s{2,}", normalized)
    segments = [" ".join(segment.split()) for segment in raw_segments if segment and segment.strip()]

    if not segments:
        segments = [" ".join(normalized.split())]

    chunks: List[str] = []
    current_words: List[str] = []

    def flush_current() -> None:
        if current_words:
            chunks.append(" ".join(current_words))
            current_words.clear()

    for segment in segments:
        words = segment.split()
        if not words:
            continue

        # Oversized segments are broken into fixed windows so duplicated long
        # passages still hash to the same chunk ids across different pages.
        if len(words) >= target_words * 2:
            flush_current()
            for start in range(0, len(words), target_words):
                window = words[start : start + target_words]
                if window:
                    chunks.append(" ".join(window))
            continue

        if current_words and len(current_words) + len(words) > target_words:
            flush_current()
        current_words.extend(words)

    flush_current()

    if not chunks:
        chunks = [" ".join(normalized.split())]

    merged_chunks: List[str] = []
    for chunk in chunks:
        words = chunk.split()
        if merged_chunks and len(words) < min_words:
            merged_chunks[-1] = f"{merged_chunks[-1]} {chunk}".strip()
        else:
            merged_chunks.append(chunk)

    return [chunk for chunk in merged_chunks if chunk]


# Groups URL records by full content hash and returns shared content plus unique chunk records.
def aggregate_content_records(
    page_records: Iterable[Dict[str, Any]],
    *,
    chunk_target_words: int = 120,
    chunk_min_words: int = 40,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for page_record in page_records:
        canonical = canonicalize_record(page_record, WEB_INDEX_COLUMNS)
        content_hash = canonical.get("content_hash")
        if not content_hash:
            continue
        # Exact duplicate pages collapse here: every URL that produced the same
        # extracted text shares one content-hash bucket.
        grouped.setdefault(content_hash, []).append(canonical)

    content_records: List[Dict[str, Any]] = []
    chunk_buckets: Dict[str, Dict[str, Any]] = {}

    for content_hash, records in grouped.items():
        first = records[0]

        # Most fields become unions across all duplicate URLs so the shared
        # content record still answers provenance questions after deduplication.
        urls = ordered_unique([value for record in records for value in _list_values(record.get("url"))])
        requested_urls = ordered_unique(
            [value for record in records for value in _list_values(record.get("requested_url"))]
        )
        domains = ordered_unique([value for record in records for value in _list_values(record.get("domain"))])
        paths = ordered_unique([value for record in records for value in _list_values(record.get("path"))])
        path_prefixes = ordered_unique(
            [value for record in records for value in _list_values(record.get("path_prefixes"))]
        )
        address_prefixes = ordered_unique(
            [value for record in records for value in _list_values(record.get("address_prefixes"))]
        )
        queries = ordered_unique([value for record in records for value in _list_values(record.get("query"))])
        snippets = ordered_unique([value for record in records for value in _list_values(record.get("snippet"))])
        sources = ordered_unique([value for record in records for value in _list_values(record.get("source"))])
        titles = ordered_unique([value for record in records for value in _list_values(record.get("title"))])
        langs = ordered_unique([value for record in records for value in _list_values(record.get("lang"))])
        dates = ordered_unique([value for record in records for value in _list_values(record.get("date"))])
        matched_rule_prefixes = ordered_unique(
            [value for record in records for value in _list_values(record.get("matched_rule_prefix"))]
        )
        matched_rule_types = ordered_unique(
            [value for record in records for value in _list_values(record.get("matched_rule_type"))]
        )
        matched_rule_address_prefixes = ordered_unique(
            [value for record in records for value in _list_values(record.get("matched_rule_address_prefix"))]
        )
        robots_txt_urls = ordered_unique(
            [value for record in records for value in _list_values(record.get("robots_txt_url"))]
        )
        policy_agents = ordered_unique(
            [value for record in records for value in _list_values(record.get("policy_agents"))]
        )

        robots_allowed_values = [bool(record.get("robots_allowed")) for record in records if record.get("robots_allowed") is not None]
        train_allowed_values = [bool(record.get("train_allowed")) for record in records if record.get("train_allowed") is not None]

        chunks = chunk_text(first.get("text") or "", target_words=chunk_target_words, min_words=chunk_min_words)
        chunk_hashes = [build_content_hash(chunk) for chunk in chunks]

        content_record = canonicalize_record(
            {
                "record_kind": "content",
                "document_id": content_hash,
                "content_id": content_hash,
                "content_hash": content_hash,
                "text": first.get("text"),
                "url": first.get("url"),
                "requested_url": first.get("requested_url"),
                "domain": first.get("domain"),
                "path": first.get("path"),
                "path_segments": first.get("path_segments"),
                "path_depth": first.get("path_depth"),
                "path_prefixes": path_prefixes,
                "address_prefixes": address_prefixes,
                "query": _first_value(records, "query"),
                "queries": queries,
                "snippet": _first_value(records, "snippet"),
                "snippets": snippets,
                "source": _first_value(records, "source"),
                "sources": sources,
                "title": _first_value(records, "title"),
                "titles": titles,
                "lang": _first_value(records, "lang"),
                "langs": langs,
                "date": _first_value(records, "date"),
                "dates": dates,
                "urls": urls,
                "requested_urls": requested_urls,
                "url_count": len(urls),
                "duplicate_url_count": max(len(urls) - 1, 0),
                "domains": domains,
                "paths": paths,
                "robots_allowed": all(robots_allowed_values) if robots_allowed_values else None,
                "robots_allowed_any": any(robots_allowed_values) if robots_allowed_values else None,
                "robots_allowed_all": all(robots_allowed_values) if robots_allowed_values else None,
                "train_allowed": all(train_allowed_values) if train_allowed_values else None,
                "train_allowed_any": any(train_allowed_values) if train_allowed_values else None,
                "train_allowed_all": all(train_allowed_values) if train_allowed_values else None,
                "matched_rule_prefix": _first_value(records, "matched_rule_prefix"),
                "matched_rule_prefixes": matched_rule_prefixes,
                "matched_rule_type": _first_value(records, "matched_rule_type"),
                "matched_rule_types": matched_rule_types,
                "matched_rule_address_prefix": _first_value(records, "matched_rule_address_prefix"),
                "matched_rule_address_prefixes": matched_rule_address_prefixes,
                "robots_txt_url": _first_value(records, "robots_txt_url"),
                "robots_txt_urls": robots_txt_urls,
                "robots_fetched_at": _first_value(records, "robots_fetched_at"),
                "robots_http_status": _first_value(records, "robots_http_status"),
                "policy_scope": _first_value(records, "policy_scope"),
                "policy_agents": policy_agents,
                "http_status": _first_value(records, "http_status"),
                "fetch_timestamp": _first_value(records, "fetch_timestamp"),
                "chunk_hashes": chunk_hashes,
                "chunk_count": len(chunk_hashes),
            },
            WEB_INDEX_COLUMNS,
        )
        content_records.append(content_record)

        for chunk_hash, chunk_text_value in zip(chunk_hashes, chunks):
            # Chunk records are global across the export, so the same repeated
            # passage referenced by different content records ends up once here.
            bucket = chunk_buckets.setdefault(
                chunk_hash,
                {
                    "record_kind": "chunk",
                    "document_id": chunk_hash,
                    "text": chunk_text_value,
                    "content_hashes": [],
                    "urls": [],
                    "requested_urls": [],
                    "domains": [],
                    "queries": [],
                    "address_prefixes": [],
                },
            )
            bucket["content_hashes"].append(content_hash)
            bucket["urls"].extend(urls)
            bucket["requested_urls"].extend(requested_urls)
            bucket["domains"].extend(domains)
            bucket["queries"].extend(queries)
            bucket["address_prefixes"].extend(address_prefixes)

    chunk_records = []
    for chunk_hash, bucket in chunk_buckets.items():
        chunk_records.append(
            canonicalize_record(
                {
                    "record_kind": "chunk",
                    # Chunk docs are identified by chunk hash rather than by any
                    # single owning page, because many contents can point to them.
                    "document_id": chunk_hash,
                    "content_id": None,
                    "content_hash": None,
                    "content_hashes": ordered_unique(bucket["content_hashes"]),
                    "content_count": len(ordered_unique(bucket["content_hashes"])),
                    "text": bucket["text"],
                    "url": bucket["urls"][0] if bucket["urls"] else None,
                    "requested_url": bucket["requested_urls"][0] if bucket["requested_urls"] else None,
                    "domain": bucket["domains"][0] if bucket["domains"] else None,
                    "query": bucket["queries"][0] if bucket["queries"] else None,
                    "urls": ordered_unique(bucket["urls"]),
                    "requested_urls": ordered_unique(bucket["requested_urls"]),
                    "domains": ordered_unique(bucket["domains"]),
                    "queries": ordered_unique(bucket["queries"]),
                    "address_prefixes": ordered_unique(bucket["address_prefixes"]),
                    "url_count": len(ordered_unique(bucket["urls"])),
                },
                WEB_INDEX_COLUMNS,
            )
        )

    return content_records, chunk_records
