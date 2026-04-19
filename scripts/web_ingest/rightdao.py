# Provides a low-rate RightDao adapter that returns deduplicated URL candidates.

import os
import time
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
class RightDaoConfig:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        method: str = "GET",
        query_param: str = "q",
        limit_param: str = "limit",
        results_key: str = "results",
        url_key: str = "url",
        title_key: str = "title",
        snippet_key: str = "snippet",
        source_name: str = "rightdao",
        timeout: int = 30,
        per_query_cap: int = 10,
        min_interval_seconds: float = 1.0,
    ) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.method = method
        self.query_param = query_param
        self.limit_param = limit_param
        self.results_key = results_key
        self.url_key = url_key
        self.title_key = title_key
        self.snippet_key = snippet_key
        self.source_name = source_name
        self.timeout = timeout
        self.per_query_cap = per_query_cap
        self.min_interval_seconds = min_interval_seconds


def _build_retry(allowed_methods: List[str]) -> Retry:
    retry_kwargs = {
        "total": 3,
        "backoff_factor": 1.0,
        "status_forcelist": [429, 500, 502, 503, 504],
    }
    try:
        return Retry(allowed_methods=allowed_methods, **retry_kwargs)
    except TypeError:  # pragma: no cover - depends on urllib3 version
        return Retry(method_whitelist=allowed_methods, **retry_kwargs)


# Wraps the RightDao HTTP API behind a conservative, retrying client.
class RightDaoClient:
    def __init__(self, config: RightDaoConfig):
        self.config = config
        self.session = requests.Session()
        self._last_request_ts = 0.0

        # Uses bounded retries so transient upstream failures do not fail a whole ingest run.
        retry = _build_retry(["GET", "POST"])
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

        # The search layer preserves snippet/title metadata because downstream
        # exports may want the original discovery context next to fetched pages.
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
            # Carry both normalized identity and discovery metadata forward so the
            # later fetch/index steps do not lose query context or snippets.
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
