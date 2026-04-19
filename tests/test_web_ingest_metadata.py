import unittest
from unittest.mock import Mock

# Covers the web-specific pieces that are easiest to regress locally:
# address-tree derivation, requested-versus-resolved URL identity, and the
# redirect path that can change robots permissions after discovery.
from scripts.web_ingest.dedup import aggregate_content_records
from scripts.web_ingest.schema import (
    WEB_INDEX_COLUMNS,
    RobotsEvaluation,
    SearchCandidate,
    build_address_prefixes,
    build_document_id,
    build_index_record,
    build_path_prefixes,
    build_path_segments,
    canonicalize_record,
)

try:
    from scripts.web_ingest.fetch_extract import PageFetcherExtractor

    FETCH_EXTRACT_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - exercised only when deps are missing
    PageFetcherExtractor = None
    FETCH_EXTRACT_IMPORT_ERROR = exc


def make_evaluation(path: str, *, robots_allowed: bool = True, matched_rule_prefix: str = "/") -> RobotsEvaluation:
    domain = "example.com"
    return RobotsEvaluation(
        domain=domain,
        path=path,
        path_segments=build_path_segments(path),
        path_depth=len(build_path_segments(path)),
        path_prefixes=build_path_prefixes(path),
        address_prefixes=build_address_prefixes(domain, path),
        robots_allowed=robots_allowed,
        train_allowed=robots_allowed,
        matched_rule_prefix=matched_rule_prefix,
        matched_rule_type="allow" if robots_allowed else "disallow",
        matched_rule_address_prefix=domain if matched_rule_prefix == "/" else f"{domain}{matched_rule_prefix}",
        robots_txt_url="https://example.com/robots.txt",
        robots_fetched_at="2026-03-27T00:00:00+00:00",
        robots_http_status=200,
        policy_scope="strict_multi",
        policy_agents=["GPTBot", "*"],
        error=None,
    )


class SchemaTests(unittest.TestCase):
    def test_build_address_prefixes_keeps_domain_tree(self):
        self.assertEqual(
            build_address_prefixes("example.com", "/dir0/dir1/dir2"),
            [
                "example.com",
                "example.com/dir0",
                "example.com/dir0/dir1",
                "example.com/dir0/dir1/dir2",
            ],
        )

    def test_canonicalize_record_derives_tree_and_permission_fields(self):
        record = canonicalize_record(
            {
                "url": "https://example.com/private/page",
                "robots_allowed": False,
                "matched_rule_prefix": "/private",
            },
            WEB_INDEX_COLUMNS,
        )

        self.assertEqual(record["requested_url"], "https://example.com/private/page")
        self.assertEqual(record["path_segments"], ["private", "page"])
        self.assertEqual(
            record["address_prefixes"],
            [
                "example.com",
                "example.com/private",
                "example.com/private/page",
            ],
        )
        self.assertFalse(record["train_allowed"])
        self.assertEqual(record["matched_rule_address_prefix"], "example.com/private")

    def test_build_index_record_uses_resolved_url_for_identity(self):
        candidate = SearchCandidate(
            query="test",
            url="https://example.com/old/path",
            snippet="candidate snippet",
        )
        evaluation = make_evaluation("/new/path")

        record = build_index_record(
            candidate,
            evaluation,
            text="  hello world  ",
            http_status=200,
            fetch_timestamp="2026-03-27T00:00:00+00:00",
            resolved_url="https://example.com/new/path",
            title="Example",
        )

        self.assertEqual(record["requested_url"], "https://example.com/old/path")
        self.assertEqual(record["url"], "https://example.com/new/path")
        self.assertEqual(record["snippet"], "candidate snippet")
        self.assertEqual(record["document_id"], build_document_id("https://example.com/new/path"))

    def test_aggregate_content_records_merges_duplicate_pages_under_one_content(self):
        first = build_index_record(
            SearchCandidate(query="one", url="https://example.com/a"),
            make_evaluation("/a"),
            text="same content across two urls",
            http_status=200,
            fetch_timestamp="2026-03-27T00:00:00+00:00",
        )
        second = build_index_record(
            SearchCandidate(query="two", url="https://example.com/b"),
            make_evaluation("/b"),
            text="same content across two urls",
            http_status=200,
            fetch_timestamp="2026-03-27T00:01:00+00:00",
        )

        content_records, chunk_records = aggregate_content_records([first, second], chunk_target_words=20, chunk_min_words=5)

        self.assertEqual(len(content_records), 1)
        self.assertEqual(content_records[0]["record_kind"], "content")
        self.assertEqual(content_records[0]["document_id"], content_records[0]["content_hash"])
        self.assertEqual(content_records[0]["url_count"], 2)
        self.assertEqual(
            content_records[0]["urls"],
            ["https://example.com/a", "https://example.com/b"],
        )
        self.assertEqual(content_records[0]["duplicate_url_count"], 1)
        self.assertGreaterEqual(len(chunk_records), 1)

    def test_aggregate_content_records_deduplicates_shared_chunks(self):
        shared_chunk = " ".join(["shared"] * 60)
        first_unique = " ".join(["first"] * 60)
        second_unique = " ".join(["second"] * 60)
        first_text = f"{shared_chunk}\n\n{first_unique}"
        second_text = f"{shared_chunk}\n\n{second_unique}"

        first = build_index_record(
            SearchCandidate(query="one", url="https://example.com/a"),
            make_evaluation("/a"),
            text=first_text,
            http_status=200,
            fetch_timestamp="2026-03-27T00:00:00+00:00",
        )
        second = build_index_record(
            SearchCandidate(query="two", url="https://example.com/b"),
            make_evaluation("/b"),
            text=second_text,
            http_status=200,
            fetch_timestamp="2026-03-27T00:01:00+00:00",
        )

        content_records, chunk_records = aggregate_content_records([first, second], chunk_target_words=60, chunk_min_words=10)

        self.assertEqual(len(content_records), 2)
        shared = [record for record in chunk_records if record["content_count"] == 2]
        self.assertEqual(len(shared), 1)
        self.assertEqual(shared[0]["record_kind"], "chunk")
        self.assertEqual(shared[0]["url_count"], 2)


@unittest.skipIf(FETCH_EXTRACT_IMPORT_ERROR is not None, f"fetch_extract import failed: {FETCH_EXTRACT_IMPORT_ERROR}")
class FetchExtractTests(unittest.TestCase):
    def test_redirected_page_rechecked_against_robots(self):
        candidate = SearchCandidate(query="test", url="https://example.com/public/doc")
        original_evaluation = make_evaluation("/public/doc")
        redirected_evaluation = make_evaluation("/private/doc", robots_allowed=False, matched_rule_prefix="/private")

        robots_cache = Mock()
        robots_cache.evaluate_url.return_value = redirected_evaluation

        fetcher = PageFetcherExtractor(robots_cache=robots_cache)
        response = Mock()
        response.status_code = 200
        response.url = "https://example.com/private/doc"
        response.content = b"<html><title>ignored</title></html>"
        response.encoding = "utf-8"
        fetcher.session = Mock()
        fetcher.session.get.return_value = response

        outcome = fetcher.fetch_and_extract(candidate, original_evaluation)

        self.assertEqual(outcome.status, "DISALLOWED")
        self.assertIsNone(outcome.record)
        self.assertEqual(outcome.status_record["requested_url"], "https://example.com/public/doc")
        self.assertEqual(outcome.status_record["url"], "https://example.com/private/doc")
        self.assertEqual(outcome.status_record["redirect_url"], "https://example.com/private/doc")
        self.assertFalse(outcome.status_record["train_allowed"])
        self.assertEqual(outcome.status_record["matched_rule_address_prefix"], "example.com/private")


if __name__ == "__main__":
    unittest.main()
