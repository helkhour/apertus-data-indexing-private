#!/usr/bin/env python3
"""Run the external `web-search` package from inside the Apertus repo."""

from __future__ import annotations

import os
import sys
from pathlib import Path


# Accept either an installed package or the intended sibling-repo layout:
# semesterProject/
#   ├── apertus-pretraining-data-indexing/
#   └── web-search/
#
# The legacy nested checkout is kept as a final fallback so the wrapper still
# works during the transition while users move the standalone repo out.
def _bootstrap_web_search_imports() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    candidate_roots: list[Path] = []

    configured_repo = os.environ.get("WEB_SEARCH_REPO")
    if configured_repo:
        candidate_roots.append(Path(configured_repo).expanduser())

    candidate_roots.append(repo_root.parent / "web-search")
    candidate_roots.append(repo_root / "web-search")

    seen_paths = set()
    for candidate_root in candidate_roots:
        src_dir = candidate_root / "src"
        if not src_dir.is_dir():
            continue
        resolved_src_dir = str(src_dir.resolve())
        if resolved_src_dir in seen_paths:
            continue
        seen_paths.add(resolved_src_dir)
        sys.path.insert(0, resolved_src_dir)


_bootstrap_web_search_imports()

try:
    from web_search import run_ingest, setup_logging, write_artifacts
    from web_search.ingest.run_web_ingest import build_parser
except ImportError as exc:  # pragma: no cover - depends on local environment
    raise SystemExit(
        "Could not import the standalone web-search package. Keep the repo beside "
        "Apertus at `../web-search`, or set WEB_SEARCH_REPO, or install it with "
        "`python3 -m pip install -e ../web-search`. "
        f"Original import error: {exc}"
    ) from exc


# Reuse the `web-search` CLI arguments verbatim so Apertus stays a thin
# integration layer rather than growing a second copy of crawl logic.
def main() -> None:
    parser = build_parser()
    parser.description = "Run web-search ingestion from Apertus and write neutral crawl artifacts"
    args = parser.parse_args()

    logger = setup_logging(args.log_level)
    artifacts = run_ingest(
        query=args.query,
        queries_file=args.queries_file,
        seed_urls_file=args.seed_urls_file,
        output_source_name=args.source_name,
        rightdao_url=args.rightdao_url,
        rightdao_api_key=args.rightdao_api_key,
        per_query_cap=args.per_query_cap,
        per_domain_cap=args.per_domain_cap,
        max_pages=args.max_pages,
        min_query_interval=args.min_query_interval,
        robots_cache_dir=args.robots_cache_dir,
        robots_cache_ttl_hours=args.robots_cache_ttl_hours,
        chunk_target_words=args.chunk_target_words,
        chunk_min_words=args.chunk_min_words,
        user_agent=args.user_agent,
        logger=logger,
    )
    write_artifacts(
        artifacts,
        output_parquet=args.output_parquet,
        chunk_output=args.chunk_output,
        status_output=args.status_output,
        robots_policy_output=args.robots_policy_output,
        logger=logger,
    )


if __name__ == "__main__":
    main()
