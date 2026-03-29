# Defines the canonical schema and normalization helpers for web ingest exports.
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

# Lists the explicit columns expected by the metadata-aware Apertus web indexer.
# `requested_url` keeps the discovery URL, while `url` is the resolved URL that
# defines document identity and the stored domain/path tree.
WEB_INDEX_COLUMNS = [
    "record_kind",
    "document_id",
    "content_id",
    "requested_url",
    "url",
    "requested_urls",
    "urls",
    "url_count",
    "duplicate_url_count",
    "domain",
    "domains",
    "path",
    "paths",
    "path_segments",
    "path_depth",
    "path_prefixes",
    "address_prefixes",
    "query",
    "queries",
    "snippet",
    "snippets",
    "source",
    "sources",
    "text",
    "title",
    "titles",
    "lang",
    "langs",
    "date",
    "dates",
    "content_hash",
    "content_hashes",
    "content_count",
    "chunk_hashes",
    "chunk_count",
    "robots_allowed",
    "robots_allowed_any",
    "robots_allowed_all",
    "train_allowed",
    "train_allowed_any",
    "train_allowed_all",
    "matched_rule_prefix",
    "matched_rule_prefixes",
    "matched_rule_type",
    "matched_rule_types",
    "matched_rule_address_prefix",
    "matched_rule_address_prefixes",
    "robots_txt_url",
    "robots_txt_urls",
    "robots_fetched_at",
    "robots_http_status",
    "policy_scope",
    "policy_agents",
    "http_status",
    "fetch_timestamp",
]

# Lists the audit trail columns written for skipped, failed, or extracted pages.
WEB_STATUS_COLUMNS = WEB_INDEX_COLUMNS + [
    "status",
    "error",
    "redirect_url",
]

# Lists the domain-level robots cache columns that can be exported separately.
ROBOTS_POLICY_COLUMNS = [
    "domain",
    "robots_txt_url",
    "robots_fetched_at",
    "robots_http_status",
    "policy_scope",
    "policy_agents",
    "robots_txt",
    "error",
]

# Carries the conservative rl-webindex robot policy agents forward into Apertus.
RL_WEBINDEX_DISALLOWED_USER_AGENTS = [
    "AI2Bot",
    "Applebot-Extended",
    "Bytespider",
    "CCBot",
    "CCBot/2.0",
    "CCBot/1.0",
    "ClaudeBot",
    "cohere-training-data-crawler",
    "Diffbot",
    "FacebookBot",
    "Meta-ExternalAgent",
    "Google-Extended",
    "GPTBot",
    "PanguBot",
    "*",
]

# Defines the default HTTP user agent for Apertus web fetches.
DEFAULT_FETCH_USER_AGENT = "ApertusWebIngest/1.0"


# Captures one URL candidate returned by RightDao or a seed file.
@dataclass
class SearchCandidate:
    query: str
    url: str
    title: Optional[str] = None
    snippet: Optional[str] = None
    source: str = "rightdao"
    rank: Optional[int] = None


# Captures the evaluated robots state for one URL.
@dataclass
class RobotsEvaluation:
    domain: str
    path: str
    path_segments: List[str]
    path_depth: int
    path_prefixes: List[str]
    address_prefixes: List[str]
    robots_allowed: bool
    train_allowed: bool
    matched_rule_prefix: Optional[str]
    matched_rule_type: Optional[str]
    matched_rule_address_prefix: Optional[str]
    robots_txt_url: str
    robots_fetched_at: Optional[str]
    robots_http_status: Optional[int]
    policy_scope: str
    policy_agents: List[str] = field(default_factory=list)
    error: Optional[str] = None


# Captures the fetch/extract result before export serialization.
@dataclass
class FetchOutcome:
    status: str
    record: Optional[Dict[str, Any]]
    status_record: Dict[str, Any]


# Returns a UTC ISO-8601 timestamp for fetch and cache bookkeeping.
def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Normalizes URLs so one Elasticsearch document maps to one stable URL identity.
def normalize_url(url: str) -> str:
    normalized = (url or "").strip()
    if not normalized:
        return ""

    parsed = urlparse(normalized)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()

    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    path = parsed.path or "/"
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


# Extracts the normalized domain from a candidate URL.
def domain_from_url(url: str) -> str:
    return urlparse(normalize_url(url)).netloc


# Extracts the normalized path from a candidate URL.
def path_from_url(url: str) -> str:
    path = urlparse(normalize_url(url)).path
    return path or "/"


# Splits the normalized path into stable directory segments.
def build_path_segments(path: str) -> List[str]:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return [part for part in normalized_path.strip("/").split("/") if part]


# Builds hierarchical path prefixes for subtree-aware filtering in Elasticsearch.
def build_path_prefixes(path: str) -> List[str]:
    normalized_path = path if path.startswith("/") else f"/{path}"
    parts = build_path_segments(normalized_path)
    prefixes = ["/"]
    current = ""

    for part in parts:
        current = f"{current}/{part}"
        prefixes.append(current)

    return prefixes


# Builds the combined domain/path tree requested for hierarchical traversal.
def build_address_prefixes(domain: str, path: str) -> List[str]:
    normalized_domain = (domain or "").strip().lower()
    if not normalized_domain:
        return []

    # The bare domain is the root node; subsequent entries let Elasticsearch
    # match an entire descendant subtree with one keyword filter.
    prefixes = [normalized_domain]
    for path_prefix in build_path_prefixes(path):
        if path_prefix == "/":
            continue
        prefixes.append(f"{normalized_domain}{path_prefix}")
    return prefixes


# Computes the path depth used by the web index mapping.
def compute_path_depth(path: str) -> int:
    return len([segment for segment in path.strip("/").split("/") if segment])


# Computes a stable content hash for duplicate-content analysis.
def build_content_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


# Computes the stable document id used as the Elasticsearch _id for web pages.
def build_document_id(url: str, explicit_document_id: Optional[str] = None) -> str:
    if explicit_document_id:
        return explicit_document_id
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


# Normalizes list-like values so parquet export stays schema-stable.
def normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)]


# Preserves first-seen order while removing duplicates from exported list fields.
def ordered_unique(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value in (None, ""):
            continue
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


# Builds the combined address where the effective robots rule was inherited from.
def build_matched_rule_address_prefix(domain: str, matched_rule_prefix: Optional[str]) -> Optional[str]:
    normalized_domain = (domain or "").strip().lower()
    if not normalized_domain or not matched_rule_prefix:
        return None
    if matched_rule_prefix == "/":
        return normalized_domain
    normalized_prefix = matched_rule_prefix if matched_rule_prefix.startswith("/") else f"/{matched_rule_prefix}"
    return f"{normalized_domain}{normalized_prefix}"


# Builds the shared record prefix used by both indexable and audit rows.
def build_base_record(
    candidate: SearchCandidate,
    evaluation: RobotsEvaluation,
    *,
    resolved_url: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "record_kind": "url",
        # Persist both URLs so we can explain redirects later without losing the
        # final canonical address that the document was actually indexed under.
        "requested_url": normalize_url(candidate.url),
        "url": normalize_url(resolved_url or candidate.url),
        "domain": evaluation.domain,
        "path": evaluation.path,
        "path_segments": evaluation.path_segments,
        "path_depth": evaluation.path_depth,
        "path_prefixes": evaluation.path_prefixes,
        "address_prefixes": evaluation.address_prefixes,
        "query": candidate.query,
        "snippet": candidate.snippet,
        "source": candidate.source,
        "robots_allowed": evaluation.robots_allowed,
        "train_allowed": evaluation.train_allowed,
        "matched_rule_prefix": evaluation.matched_rule_prefix,
        "matched_rule_type": evaluation.matched_rule_type,
        "matched_rule_address_prefix": evaluation.matched_rule_address_prefix,
        "robots_txt_url": evaluation.robots_txt_url,
        "robots_fetched_at": evaluation.robots_fetched_at,
        "robots_http_status": evaluation.robots_http_status,
        "policy_scope": evaluation.policy_scope,
        "policy_agents": evaluation.policy_agents,
    }


# Builds one audit/status row regardless of whether extraction succeeded.
def build_status_record(
    candidate: SearchCandidate,
    evaluation: RobotsEvaluation,
    status: str,
    *,
    resolved_url: Optional[str] = None,
    error: Optional[str] = None,
    redirect_url: Optional[str] = None,
    http_status: Optional[int] = None,
    fetch_timestamp: Optional[str] = None,
    document_id: Optional[str] = None,
    text: Optional[str] = None,
    title: Optional[str] = None,
    lang: Optional[str] = None,
    date: Optional[str] = None,
    content_hash: Optional[str] = None,
) -> Dict[str, Any]:
    record = build_base_record(candidate, evaluation, resolved_url=resolved_url)
    record.update(
        {
            "document_id": document_id,
            "text": text,
            "title": title,
            "lang": lang,
            "date": date,
            "content_hash": content_hash,
            "http_status": http_status,
            "fetch_timestamp": fetch_timestamp,
            "status": status,
            "error": error,
            "redirect_url": redirect_url,
        }
    )
    return canonicalize_record(record, WEB_STATUS_COLUMNS)


# Builds one indexable page record for successful extractions.
def build_index_record(
    candidate: SearchCandidate,
    evaluation: RobotsEvaluation,
    *,
    text: str,
    http_status: Optional[int],
    fetch_timestamp: str,
    resolved_url: Optional[str] = None,
    title: Optional[str] = None,
    lang: Optional[str] = None,
    date: Optional[str] = None,
) -> Dict[str, Any]:
    clean_text = " ".join((text or "").split())
    resolved_document_url = normalize_url(resolved_url or candidate.url)
    record = build_base_record(candidate, evaluation, resolved_url=resolved_document_url)
    record.update(
        {
            "document_id": build_document_id(resolved_document_url),
            "text": clean_text,
            "title": title,
            "lang": lang,
            "date": date,
            "content_hash": build_content_hash(clean_text),
            "http_status": http_status,
            "fetch_timestamp": fetch_timestamp,
        }
    )
    return canonicalize_record(record, WEB_INDEX_COLUMNS)


# Canonicalizes exported rows so missing columns become explicit nulls or empty lists.
def canonicalize_record(record: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
    normalized = {column: None for column in columns}
    normalized.update(record)
    normalized["record_kind"] = normalized.get("record_kind") or "content"
    # Recompute tree fields from the stored URL so partially populated records
    # still end up with a self-consistent domain/path hierarchy.
    normalized["requested_url"] = normalize_url(normalized.get("requested_url") or normalized.get("url") or "")
    normalized["url"] = normalize_url(normalized.get("url") or "")
    normalized["requested_urls"] = ordered_unique(
        normalize_list(normalized.get("requested_urls")) + normalize_list(normalized.get("requested_url"))
    )
    normalized["urls"] = ordered_unique(
        normalize_list(normalized.get("urls")) + normalize_list(normalized.get("url"))
    )
    normalized["url_count"] = normalized.get("url_count")
    if normalized["url_count"] is None:
        normalized["url_count"] = len(normalized["urls"])
    normalized["duplicate_url_count"] = normalized.get("duplicate_url_count")
    if normalized["duplicate_url_count"] is None:
        normalized["duplicate_url_count"] = max(int(normalized["url_count"]) - 1, 0)
    normalized["path"] = normalized.get("path") or path_from_url(normalized.get("url") or "")
    normalized["domain"] = normalized.get("domain") or domain_from_url(normalized.get("url") or "")
    normalized["domains"] = ordered_unique(
        normalize_list(normalized.get("domains")) + normalize_list(normalized.get("domain"))
    )
    normalized["paths"] = ordered_unique(
        normalize_list(normalized.get("paths")) + normalize_list(normalized.get("path"))
    )
    normalized["path_segments"] = normalize_list(
        normalized.get("path_segments") or build_path_segments(normalized["path"])
    )
    normalized["path_depth"] = normalized.get("path_depth")
    if normalized["path_depth"] is None:
        normalized["path_depth"] = compute_path_depth(normalized["path"])
    normalized["path_prefixes"] = normalize_list(
        normalized.get("path_prefixes") or build_path_prefixes(normalized["path"])
    )
    normalized["queries"] = ordered_unique(
        normalize_list(normalized.get("queries")) + normalize_list(normalized.get("query"))
    )
    normalized["snippets"] = ordered_unique(
        normalize_list(normalized.get("snippets")) + normalize_list(normalized.get("snippet"))
    )
    normalized["sources"] = ordered_unique(
        normalize_list(normalized.get("sources")) + normalize_list(normalized.get("source"))
    )
    normalized["titles"] = ordered_unique(
        normalize_list(normalized.get("titles")) + normalize_list(normalized.get("title"))
    )
    normalized["langs"] = ordered_unique(
        normalize_list(normalized.get("langs")) + normalize_list(normalized.get("lang"))
    )
    normalized["dates"] = ordered_unique(
        normalize_list(normalized.get("dates")) + normalize_list(normalized.get("date"))
    )
    normalized["address_prefixes"] = normalize_list(
        normalized.get("address_prefixes") or build_address_prefixes(normalized["domain"], normalized["path"])
    )
    # `train_allowed` currently mirrors the robots decision, but keeping it as a
    # separate field leaves room for future policy layers beyond robots.txt.
    if normalized.get("train_allowed") is None:
        normalized["train_allowed"] = normalized.get("robots_allowed")
    if normalized.get("robots_allowed_any") is None:
        normalized["robots_allowed_any"] = normalized.get("robots_allowed")
    if normalized.get("robots_allowed_all") is None:
        normalized["robots_allowed_all"] = normalized.get("robots_allowed")
    if normalized.get("train_allowed_any") is None:
        normalized["train_allowed_any"] = normalized.get("train_allowed")
    if normalized.get("train_allowed_all") is None:
        normalized["train_allowed_all"] = normalized.get("train_allowed")
    if not normalized.get("matched_rule_address_prefix"):
        normalized["matched_rule_address_prefix"] = build_matched_rule_address_prefix(
            normalized["domain"],
            normalized.get("matched_rule_prefix"),
        )
    normalized["matched_rule_prefixes"] = ordered_unique(
        normalize_list(normalized.get("matched_rule_prefixes")) + normalize_list(normalized.get("matched_rule_prefix"))
    )
    normalized["matched_rule_types"] = ordered_unique(
        normalize_list(normalized.get("matched_rule_types")) + normalize_list(normalized.get("matched_rule_type"))
    )
    normalized["matched_rule_address_prefixes"] = ordered_unique(
        normalize_list(normalized.get("matched_rule_address_prefixes"))
        + normalize_list(normalized.get("matched_rule_address_prefix"))
    )
    normalized["robots_txt_urls"] = ordered_unique(
        normalize_list(normalized.get("robots_txt_urls")) + normalize_list(normalized.get("robots_txt_url"))
    )
    normalized["policy_agents"] = normalize_list(normalized.get("policy_agents"))
    if normalized.get("record_kind") == "content":
        normalized["content_id"] = (
            normalized.get("content_id") or normalized.get("content_hash") or normalized.get("document_id")
        )
    normalized["content_hashes"] = ordered_unique(
        normalize_list(normalized.get("content_hashes")) + normalize_list(normalized.get("content_hash"))
    )
    normalized["content_count"] = normalized.get("content_count")
    if normalized["content_count"] is None:
        normalized["content_count"] = len(normalized["content_hashes"]) or (1 if normalized.get("content_hash") else 0)
    normalized["chunk_hashes"] = ordered_unique(normalize_list(normalized.get("chunk_hashes")))
    normalized["chunk_count"] = normalized.get("chunk_count")
    if normalized["chunk_count"] is None:
        normalized["chunk_count"] = len(normalized["chunk_hashes"])
    return normalized
