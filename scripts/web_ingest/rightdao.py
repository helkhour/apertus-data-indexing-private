# Provides a low-rate RightDao adapter that returns deduplicated URL candidates.
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if __package__:
    # Reuses the canonical candidate and URL normalization helpers when imported as a package.
    from .schema import SearchCandidate, normalize_url
else:
    # Reuses the canonical candidate and URL normalization helpers when run as a script.
    from schema import SearchCandidate, normalize_url


# Collects the configurable RightDao API settings in one place.
@dataclass
class RightDaoConfig:
    base_url: str
    api_key: Optional[str] = None
    method: str = "GET"
    query_param: str = "q"
    limit_param: str = "limit"
    results_key: str = "results"
    url_key: str = "url"
    title_key: str = "title"
    snippet_key: str = "snippet"
    source_name: str = "rightdao"
    timeout: int = 30
    per_query_cap: int = 10
    min_interval_seconds: float = 1.0


# Wraps the RightDao HTTP API behind a conservative, retrying client.
class RightDaoClient:
    def __init__(self, config: RightDaoConfig):
        self.config = config
        self.session = requests.Session()
        self._last_request_ts = 0.0

        # Uses bounded retries so transient upstream failures do not fail a whole ingest run.
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # Builds a client from CLI arguments or environment variables.
    @classmethod
    def from_args(
        cls,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        per_query_cap: int = 10,
        min_interval_seconds: float = 1.0,
    ) -> "RightDaoClient":
        config = RightDaoConfig(
            base_url=base_url or os.environ.get("RIGHTDAO_URL", ""),
            api_key=api_key or os.environ.get("RIGHTDAO_API_KEY"),
            method=os.environ.get("RIGHTDAO_METHOD", "GET").upper(),
            query_param=os.environ.get("RIGHTDAO_QUERY_PARAM", "q"),
            limit_param=os.environ.get("RIGHTDAO_LIMIT_PARAM", "limit"),
            results_key=os.environ.get("RIGHTDAO_RESULTS_KEY", "results"),
            url_key=os.environ.get("RIGHTDAO_URL_KEY", "url"),
            title_key=os.environ.get("RIGHTDAO_TITLE_KEY", "title"),
            snippet_key=os.environ.get("RIGHTDAO_SNIPPET_KEY", "snippet"),
            source_name=os.environ.get("RIGHTDAO_SOURCE", "rightdao"),
            timeout=int(os.environ.get("RIGHTDAO_TIMEOUT", "30")),
            per_query_cap=per_query_cap,
            min_interval_seconds=min_interval_seconds,
        )
        return cls(config)

    # Executes one capped search query and returns normalized URL candidates.
    def search(self, query: str, limit: Optional[int] = None) -> List[SearchCandidate]:
        if not self.config.base_url:
            raise ValueError("RightDao base URL is required for live query ingestion")

        capped_limit = min(limit or self.config.per_query_cap, self.config.per_query_cap)
        self._respect_rate_limit()

        params = {
            self.config.query_param: query,
            self.config.limit_param: capped_limit,
        }
        headers = {"Accept": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        if self.config.method == "POST":
            response = self.session.post(
                self.config.base_url,
                json=params,
                headers=headers,
                timeout=self.config.timeout,
            )
        else:
            response = self.session.get(
                self.config.base_url,
                params=params,
                headers=headers,
                timeout=self.config.timeout,
            )

        self._last_request_ts = time.time()
        response.raise_for_status()
        payload = response.json()
        return self._extract_candidates(query, payload, capped_limit)

    # Enforces a minimum interval between RightDao requests to avoid aggressive querying.
    def _respect_rate_limit(self) -> None:
        wait_seconds = self.config.min_interval_seconds - (time.time() - self._last_request_ts)
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    # Extracts the configured results list from a RightDao response payload.
    def _extract_results(self, payload: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get(self.config.results_key, [])
        return []

    # Normalizes and deduplicates URL candidates emitted for one query.
    def _extract_candidates(self, query: str, payload: Any, limit: int) -> List[SearchCandidate]:
        seen_urls = set()
        candidates: List[SearchCandidate] = []

        for rank, raw_result in enumerate(self._extract_results(payload), start=1):
            raw_url = (raw_result.get(self.config.url_key) or "").strip()
            normalized_url = normalize_url(raw_url)
            if not normalized_url or normalized_url in seen_urls:
                continue

            seen_urls.add(normalized_url)
            candidates.append(
                SearchCandidate(
                    query=query,
                    url=normalized_url,
                    title=raw_result.get(self.config.title_key),
                    snippet=raw_result.get(self.config.snippet_key),
                    source=self.config.source_name,
                    rank=rank,
                )
            )
            if len(candidates) >= limit:
                break

        return candidates
