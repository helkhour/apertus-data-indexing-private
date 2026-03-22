# Orchestrates query -> candidate URLs -> robots check -> fetch/extract -> parquet export.
from __future__ import annotations

import argparse
import csv
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Iterable, List

if __package__:
    # Reuses the local web-ingest modules when imported as a package.
    from .export_web_parquet import write_jsonl, write_parquet
    from .fetch_extract import PageFetcherExtractor
    from .rightdao import RightDaoClient
    from .robots import RobotsCache
    from .schema import (
        DEFAULT_FETCH_USER_AGENT,
        ROBOTS_POLICY_COLUMNS,
        WEB_INDEX_COLUMNS,
        WEB_STATUS_COLUMNS,
        SearchCandidate,
        build_status_record,
        domain_from_url,
        normalize_url,
    )
else:
    # Reuses the local web-ingest modules when run directly as a script.
    from export_web_parquet import write_jsonl, write_parquet
    from fetch_extract import PageFetcherExtractor
    from rightdao import RightDaoClient
    from robots import RobotsCache
    from schema import (
        DEFAULT_FETCH_USER_AGENT,
        ROBOTS_POLICY_COLUMNS,
        WEB_INDEX_COLUMNS,
        WEB_STATUS_COLUMNS,
        SearchCandidate,
        build_status_record,
        domain_from_url,
        normalize_url,
    )


# Configures consistent CLI logging for the web ingest pipeline.
def setup_logging(log_level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


# Loads raw query strings from a newline-delimited file.
def load_queries(query_file: Path) -> List[str]:
    return [line.strip() for line in query_file.read_text().splitlines() if line.strip()]


# Loads seed candidates from JSONL, CSV, TSV, or plain text.
def load_seed_candidates(seed_file: Path, default_source: str) -> List[SearchCandidate]:
    suffix = seed_file.suffix.lower()
    candidates: List[SearchCandidate] = []

    if suffix == ".jsonl":
        for line in seed_file.read_text().splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            candidates.append(
                SearchCandidate(
                    query=payload.get("query", ""),
                    url=payload["url"],
                    title=payload.get("title"),
                    snippet=payload.get("snippet"),
                    source=payload.get("source", default_source),
                    rank=payload.get("rank"),
                )
            )
        return candidates

    if suffix in {".csv", ".tsv"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        with seed_file.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            for row in reader:
                candidates.append(
                    SearchCandidate(
                        query=row.get("query", ""),
                        url=row["url"],
                        title=row.get("title"),
                        snippet=row.get("snippet"),
                        source=row.get("source", default_source),
                        rank=int(row["rank"]) if row.get("rank") else None,
                    )
                )
        return candidates

    for line in seed_file.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "\t" in stripped:
            query, url = stripped.split("\t", 1)
        else:
            query, url = "", stripped
        candidates.append(SearchCandidate(query=query.strip(), url=url.strip(), source=default_source))
    return candidates


# Applies global URL deduplication and conservative per-domain caps before fetching pages.
def deduplicate_candidates(
    candidates: Iterable[SearchCandidate],
    *,
    per_domain_cap: int,
    max_pages: int,
) -> List[SearchCandidate]:
    seen_urls = set()
    domain_counts = Counter()
    deduplicated: List[SearchCandidate] = []

    for candidate in candidates:
        normalized_url = normalize_url(candidate.url)
        if not normalized_url or normalized_url in seen_urls:
            continue

        domain = domain_from_url(normalized_url)
        if per_domain_cap > 0 and domain_counts[domain] >= per_domain_cap:
            continue

        seen_urls.add(normalized_url)
        domain_counts[domain] += 1
        deduplicated.append(
            SearchCandidate(
                query=candidate.query,
                url=normalized_url,
                title=candidate.title,
                snippet=candidate.snippet,
                source=candidate.source,
                rank=candidate.rank,
            )
        )
        if max_pages > 0 and len(deduplicated) >= max_pages:
            break

    return deduplicated


# Collects candidates either from RightDao queries or a seed URL file.
def collect_candidates(args, logger: logging.Logger) -> List[SearchCandidate]:
    if args.seed_urls_file:
        logger.info("Loading candidate URLs from seed file")
        return load_seed_candidates(Path(args.seed_urls_file), args.source_name)

    queries = list(args.query or [])
    if args.queries_file:
        queries.extend(load_queries(Path(args.queries_file)))

    if not queries:
        raise ValueError("Provide --query, --queries-file, or --seed-urls-file")

    logger.info("Querying RightDao for candidate URLs")
    client = RightDaoClient.from_args(
        base_url=args.rightdao_url,
        api_key=args.rightdao_api_key,
        per_query_cap=args.per_query_cap,
        min_interval_seconds=args.min_query_interval,
    )
    candidates: List[SearchCandidate] = []
    for query in queries:
        candidates.extend(client.search(query, limit=args.per_query_cap))
    return candidates


# Runs the full web ingest flow and writes both indexable rows and audit trails.
def main() -> None:
    parser = argparse.ArgumentParser(description="Run Apertus web ingestion and export parquet for indexing")
    parser.add_argument("--query", action="append", help="One query string to send to RightDao")
    parser.add_argument("--queries-file", help="Text file with one query per line")
    parser.add_argument("--seed-urls-file", help="Optional seed URL file for fixture or replay ingestion")
    parser.add_argument("--output-parquet", required=True, help="Parquet file containing indexable page rows")
    parser.add_argument("--status-output", help="Optional parquet or jsonl file containing all page statuses")
    parser.add_argument("--robots-policy-output", help="Optional parquet or jsonl file containing robots cache rows")
    parser.add_argument("--rightdao-url", help="RightDao HTTP endpoint to query for candidate URLs")
    parser.add_argument("--rightdao-api-key", help="Optional RightDao bearer token")
    parser.add_argument("--source-name", default="rightdao", help="Source label stored with exported rows")
    parser.add_argument("--per-query-cap", type=int, default=10, help="Maximum RightDao URLs to keep per query")
    parser.add_argument("--per-domain-cap", type=int, default=5, help="Maximum kept URLs per domain before fetch")
    parser.add_argument("--max-pages", type=int, default=100, help="Maximum total pages to fetch in one run")
    parser.add_argument("--min-query-interval", type=float, default=1.0, help="Minimum seconds between RightDao requests")
    parser.add_argument("--robots-cache-dir", help="Optional directory used for cached robots.txt responses")
    parser.add_argument("--robots-cache-ttl-hours", type=int, default=24, help="How long cached robots.txt files stay fresh")
    parser.add_argument("--user-agent", default=DEFAULT_FETCH_USER_AGENT, help="HTTP user agent used for robots and page fetches")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logger = setup_logging(args.log_level)
    candidates = collect_candidates(args, logger)
    candidates = deduplicate_candidates(
        candidates,
        per_domain_cap=args.per_domain_cap,
        max_pages=args.max_pages,
    )

    logger.info("Collected %s deduplicated candidate URLs", len(candidates))
    robots_cache = RobotsCache(
        cache_dir=Path(args.robots_cache_dir) if args.robots_cache_dir else None,
        ttl_seconds=args.robots_cache_ttl_hours * 3600,
        user_agent=args.user_agent,
    )
    fetcher = PageFetcherExtractor(user_agent=args.user_agent)

    index_records = []
    status_records = []

    for candidate in candidates:
        evaluation = robots_cache.evaluate_url(candidate.url)
        if not evaluation.robots_allowed:
            logger.info("Skipping disallowed URL: %s", candidate.url)
            status_records.append(
                build_status_record(
                    candidate,
                    evaluation,
                    "DISALLOWED",
                    error="Rejected by robots policy",
                )
            )
            continue

        outcome = fetcher.fetch_and_extract(candidate, evaluation)
        status_records.append(outcome.status_record)
        if outcome.record is not None:
            index_records.append(outcome.record)

    logger.info("Writing %s indexable records", len(index_records))
    write_parquet(index_records, Path(args.output_parquet), WEB_INDEX_COLUMNS)

    if args.status_output:
        logger.info("Writing %s status rows", len(status_records))
        status_path = Path(args.status_output)
        if status_path.suffix.lower() == ".jsonl":
            write_jsonl(status_records, status_path)
        else:
            write_parquet(status_records, status_path, WEB_STATUS_COLUMNS)

    if args.robots_policy_output:
        robots_records = robots_cache.export_records()
        robots_path = Path(args.robots_policy_output)
        if robots_path.suffix.lower() == ".jsonl":
            write_jsonl(robots_records, robots_path)
        else:
            write_parquet(robots_records, robots_path, ROBOTS_POLICY_COLUMNS)


# Runs the orchestrator when the module is executed directly.
if __name__ == "__main__":
    main()
