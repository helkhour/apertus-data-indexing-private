# Defines the canonical schema and normalization helpers for web ingest exports.
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

# Lists the explicit columns expected by the metadata-aware Apertus web indexer.
WEB_INDEX_COLUMNS = [
    "document_id",
    "url",
    "domain",
    "path",
    "path_depth",
    "path_prefixes",
    "query",
    "source",
    "text",
    "title",
    "lang",
    "date",
    "content_hash",
    "robots_allowed",
    "matched_rule_prefix",
    "http_status",
    "fetch_timestamp",
]

# Lists the audit trail columns written for skipped, failed, or extracted pages.
WEB_STATUS_COLUMNS = WEB_INDEX_COLUMNS + [
    "status",
    "error",
    "redirect_url",
    "robots_txt_url",
    "robots_fetched_at",
    "policy_scope",
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
    path_depth: int
    path_prefixes: List[str]
    robots_allowed: bool
    matched_rule_prefix: Optional[str]
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


# Builds hierarchical path prefixes for subtree-aware filtering in Elasticsearch.
def build_path_prefixes(path: str) -> List[str]:
    normalized_path = path if path.startswith("/") else f"/{path}"
    parts = [part for part in normalized_path.strip("/").split("/") if part]
    prefixes = ["/"]
    current = ""

    for part in parts:
        current = f"{current}/{part}"
        prefixes.append(current)

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


# Builds the shared record prefix used by both indexable and audit rows.
def build_base_record(candidate: SearchCandidate, evaluation: RobotsEvaluation) -> Dict[str, Any]:
    return {
        "url": normalize_url(candidate.url),
        "domain": evaluation.domain,
        "path": evaluation.path,
        "path_depth": evaluation.path_depth,
        "path_prefixes": evaluation.path_prefixes,
        "query": candidate.query,
        "source": candidate.source,
        "robots_allowed": evaluation.robots_allowed,
        "matched_rule_prefix": evaluation.matched_rule_prefix,
    }


# Builds one audit/status row regardless of whether extraction succeeded.
def build_status_record(
    candidate: SearchCandidate,
    evaluation: RobotsEvaluation,
    status: str,
    *,
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
    record = build_base_record(candidate, evaluation)
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
            "robots_txt_url": evaluation.robots_txt_url,
            "robots_fetched_at": evaluation.robots_fetched_at,
            "policy_scope": evaluation.policy_scope,
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
    title: Optional[str] = None,
    lang: Optional[str] = None,
    date: Optional[str] = None,
) -> Dict[str, Any]:
    clean_text = " ".join((text or "").split())
    record = build_base_record(candidate, evaluation)
    record.update(
        {
            "document_id": build_document_id(candidate.url),
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
    normalized["url"] = normalize_url(normalized.get("url") or "")
    normalized["path"] = normalized.get("path") or path_from_url(normalized.get("url") or "")
    normalized["domain"] = normalized.get("domain") or domain_from_url(normalized.get("url") or "")
    normalized["path_depth"] = normalized.get("path_depth")
    if normalized["path_depth"] is None:
        normalized["path_depth"] = compute_path_depth(normalized["path"])
    normalized["path_prefixes"] = normalize_list(
        normalized.get("path_prefixes") or build_path_prefixes(normalized["path"])
    )
    return normalized
