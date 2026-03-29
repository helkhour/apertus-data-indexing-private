# Fetches allowed pages and extracts readable text and metadata with Trafilatura.
from __future__ import annotations

import re
from typing import Optional

import requests
import trafilatura
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if __package__:
    # Reuses the canonical export schema and helpers when imported as a package.
    from .schema import (
        DEFAULT_FETCH_USER_AGENT,
        FetchOutcome,
        RobotsEvaluation,
        SearchCandidate,
        build_index_record,
        build_status_record,
        normalize_url,
        utcnow_iso,
    )
else:
    # Reuses the canonical export schema and helpers when run as a script.
    from schema import (
        DEFAULT_FETCH_USER_AGENT,
        FetchOutcome,
        RobotsEvaluation,
        SearchCandidate,
        build_index_record,
        build_status_record,
        normalize_url,
        utcnow_iso,
    )


# Wraps HTTP fetching and Trafilatura extraction behind a bounded, retrying client.
class PageFetcherExtractor:
    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_FETCH_USER_AGENT,
        robots_cache=None,
        timeout: int = 30,
        max_bytes: int = 5_000_000,
    ) -> None:
        self.user_agent = user_agent
        self.robots_cache = robots_cache
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.session = requests.Session()

        # Uses bounded retries so transient page fetch failures do not fail a whole ingest run.
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    # Fetches one candidate URL and returns an indexable record plus an audit row.
    def fetch_and_extract(self, candidate: SearchCandidate, evaluation: RobotsEvaluation) -> FetchOutcome:
        fetch_timestamp = utcnow_iso()
        headers = {"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml"}

        try:
            response = self.session.get(
                candidate.url,
                headers=headers,
                timeout=self.timeout,
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            status_record = build_status_record(
                candidate,
                evaluation,
                "FAILED",
                resolved_url=candidate.url,
                error=str(exc),
                fetch_timestamp=fetch_timestamp,
            )
            return FetchOutcome(status="FAILED", record=None, status_record=status_record)

        # The final response URL becomes the canonical stored address. A search
        # result can point at one URL while the fetched document lives elsewhere
        # after redirects.
        resolved_url = normalize_url(response.url or candidate.url)
        normalized_candidate_url = normalize_url(candidate.url)
        # Redirects can also move the page into a different robots subtree, so
        # the effective permission metadata must be recomputed on the final URL.
        effective_evaluation = self._resolve_evaluation(
            evaluation,
            requested_url=normalized_candidate_url,
            resolved_url=resolved_url,
        )
        redirect_url = resolved_url if resolved_url != normalized_candidate_url else None

        if response.status_code >= 400:
            status_record = build_status_record(
                candidate,
                effective_evaluation,
                "FAILED",
                resolved_url=resolved_url,
                error=f"HTTP {response.status_code}",
                http_status=response.status_code,
                fetch_timestamp=fetch_timestamp,
                redirect_url=redirect_url,
            )
            return FetchOutcome(status="FAILED", record=None, status_record=status_record)

        if not effective_evaluation.robots_allowed:
            # Stop here instead of indexing metadata from the discovery URL when
            # the final destination is disallowed by robots.txt.
            status_record = build_status_record(
                candidate,
                effective_evaluation,
                "DISALLOWED",
                resolved_url=resolved_url,
                error="Redirected or resolved URL is disallowed by robots policy",
                http_status=response.status_code,
                fetch_timestamp=fetch_timestamp,
                redirect_url=redirect_url,
            )
            return FetchOutcome(status="DISALLOWED", record=None, status_record=status_record)

        # Decodes only a bounded amount of body bytes to keep extraction predictable.
        html = response.content[: self.max_bytes].decode(response.encoding or "utf-8", errors="ignore")
        text = trafilatura.extract(
            filecontent=html,
            url=resolved_url,
            output_format="txt",
            include_comments=False,
            include_tables=False,
        )
        title = self._extract_title(html)
        lang = self._extract_lang(html)
        date = self._extract_date(html)

        if not text or not text.strip():
            status_record = build_status_record(
                candidate,
                effective_evaluation,
                "EMPTY_EXTRACTION",
                resolved_url=resolved_url,
                error="Trafilatura returned no main text",
                http_status=response.status_code,
                fetch_timestamp=fetch_timestamp,
                redirect_url=redirect_url,
                title=title,
                lang=lang,
                date=date,
            )
            return FetchOutcome(status="EMPTY_EXTRACTION", record=None, status_record=status_record)

        record = build_index_record(
            candidate,
            effective_evaluation,
            text=text,
            http_status=response.status_code,
            fetch_timestamp=fetch_timestamp,
            resolved_url=resolved_url,
            title=title,
            lang=lang,
            date=date,
        )
        status_record = build_status_record(
            candidate,
            effective_evaluation,
            "FOUND",
            resolved_url=resolved_url,
            http_status=response.status_code,
            fetch_timestamp=fetch_timestamp,
            redirect_url=redirect_url,
            document_id=record["document_id"],
            text=record["text"],
            title=title,
            lang=lang,
            date=date,
            content_hash=record["content_hash"],
        )
        return FetchOutcome(status="FOUND", record=record, status_record=status_record)

    # Re-evaluates redirects so the stored tree and permissions match the resolved URL.
    def _resolve_evaluation(
        self,
        evaluation: RobotsEvaluation,
        *,
        requested_url: str,
        resolved_url: str,
    ) -> RobotsEvaluation:
        if not self.robots_cache:
            return evaluation
        if normalize_url(resolved_url) == normalize_url(requested_url):
            return evaluation
        return self.robots_cache.evaluate_url(resolved_url)

    # Extracts the HTML title without adding a new parser dependency.
    def _extract_title(self, html: str) -> Optional[str]:
        match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return " ".join(match.group(1).split())

    # Extracts the declared document language from the html tag.
    def _extract_lang(self, html: str) -> Optional[str]:
        match = re.search(r"<html[^>]*\blang=[\"']?([^\"' >]+)", html, flags=re.IGNORECASE)
        return match.group(1).strip() if match else None

    # Extracts a date-like meta field from common article metadata tags.
    def _extract_date(self, html: str) -> Optional[str]:
        patterns = [
            r"<meta[^>]+property=[\"']article:published_time[\"'][^>]+content=[\"']([^\"']+)",
            r"<meta[^>]+name=[\"']pubdate[\"'][^>]+content=[\"']([^\"']+)",
            r"<time[^>]+datetime=[\"']([^\"']+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, html, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
