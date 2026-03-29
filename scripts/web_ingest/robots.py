# Fetches, caches, and evaluates robots.txt policies using the rl-webindex Protego flow.
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from protego import Protego
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if __package__:
    # Reuses the canonical robots schema and URL helpers when imported as a package.
    from .schema import (
        DEFAULT_FETCH_USER_AGENT,
        RL_WEBINDEX_DISALLOWED_USER_AGENTS,
        ROBOTS_POLICY_COLUMNS,
        RobotsEvaluation,
        build_address_prefixes,
        build_matched_rule_address_prefix,
        build_path_prefixes,
        build_path_segments,
        compute_path_depth,
        domain_from_url,
        path_from_url,
        utcnow_iso,
    )
else:
    # Reuses the canonical robots schema and URL helpers when run as a script.
    from schema import (
        DEFAULT_FETCH_USER_AGENT,
        RL_WEBINDEX_DISALLOWED_USER_AGENTS,
        ROBOTS_POLICY_COLUMNS,
        RobotsEvaluation,
        build_address_prefixes,
        build_matched_rule_address_prefix,
        build_path_prefixes,
        build_path_segments,
        compute_path_depth,
        domain_from_url,
        path_from_url,
        utcnow_iso,
    )


# Stores one cached robots.txt fetch result per domain.
class RobotsCache:
    def __init__(
        self,
        *,
        cache_dir: Optional[Path] = None,
        ttl_seconds: int = 24 * 3600,
        user_agent: str = DEFAULT_FETCH_USER_AGENT,
        policy_agents: Optional[List[str]] = None,
        timeout: int = 20,
    ) -> None:
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.ttl_seconds = ttl_seconds
        self.user_agent = user_agent
        self.policy_agents = policy_agents or list(RL_WEBINDEX_DISALLOWED_USER_AGENTS)
        self.timeout = timeout
        self._memory_cache: Dict[str, Dict[str, Any]] = {}
        self.session = requests.Session()

        # Uses bounded retries so transient robots fetch failures do not spin forever.
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    # Evaluates one URL against the cached or freshly fetched robots policy.
    def evaluate_url(self, url: str) -> RobotsEvaluation:
        domain = domain_from_url(url)
        path = path_from_url(url)
        path_segments = build_path_segments(path)
        path_prefixes = build_path_prefixes(path)
        address_prefixes = build_address_prefixes(domain, path)
        cache_entry = self._get_or_fetch(domain)
        robots_txt = cache_entry.get("robots_txt")

        if not robots_txt:
            # Missing robots.txt is treated as allowed, but the caller still gets
            # the tree and source metadata describing which policy URL was checked.
            return RobotsEvaluation(
                domain=domain,
                path=path,
                path_segments=path_segments,
                path_depth=compute_path_depth(path),
                path_prefixes=path_prefixes,
                address_prefixes=address_prefixes,
                robots_allowed=True,
                train_allowed=True,
                matched_rule_prefix=None,
                matched_rule_type=None,
                matched_rule_address_prefix=None,
                robots_txt_url=cache_entry["robots_txt_url"],
                robots_fetched_at=cache_entry.get("robots_fetched_at"),
                robots_http_status=cache_entry.get("robots_http_status"),
                policy_scope=cache_entry.get("policy_scope", "strict_multi"),
                policy_agents=self.policy_agents,
                error=cache_entry.get("error"),
            )

        parser = Protego.parse(robots_txt)

        # Evaluates all rl-webindex policy agents and keeps the strictest outcome.
        agent_evaluations = []
        for agent in self.policy_agents:
            allowed = parser.can_fetch(path, agent)
            matched_prefix, matched_allowed = self._match_rule_prefix(robots_txt, path, agent)
            agent_evaluations.append((agent, allowed, matched_prefix, matched_allowed))

        blocked = [item for item in agent_evaluations if not item[1]]
        matched_rule_prefix = None
        matched_rule_type = None
        if blocked:
            # Keep the longest blocking prefix because that is the most specific
            # subtree rule explaining why this URL cannot be used for training.
            blocked_prefixes = [item[2] or "/" for item in blocked]
            matched_rule_prefix = max(blocked_prefixes, key=len)
            matched_rule_type = "disallow"
        else:
            allowed_prefixes = [item[2] for item in agent_evaluations if item[2]]
            matched_rule_prefix = max(allowed_prefixes, key=len) if allowed_prefixes else None
            matched_rule_type = "allow" if matched_rule_prefix else None

        return RobotsEvaluation(
            domain=domain,
            path=path,
            path_segments=path_segments,
            path_depth=compute_path_depth(path),
            path_prefixes=path_prefixes,
            address_prefixes=address_prefixes,
            robots_allowed=not blocked,
            train_allowed=not blocked,
            matched_rule_prefix=matched_rule_prefix,
            matched_rule_type=matched_rule_type,
            matched_rule_address_prefix=build_matched_rule_address_prefix(domain, matched_rule_prefix),
            robots_txt_url=cache_entry["robots_txt_url"],
            robots_fetched_at=cache_entry.get("robots_fetched_at"),
            robots_http_status=cache_entry.get("robots_http_status"),
            policy_scope=cache_entry.get("policy_scope", "strict_multi"),
            policy_agents=self.policy_agents,
            error=cache_entry.get("error"),
        )

    # Exports the in-memory robots cache in a parquet-friendly row format.
    def export_records(self) -> List[Dict[str, Any]]:
        records = []
        for domain, entry in self._memory_cache.items():
            # This export is domain scoped rather than page scoped because one
            # cached robots policy applies to every path underneath that domain.
            records.append(
                {
                    "domain": domain,
                    "robots_txt_url": entry.get("robots_txt_url"),
                    "robots_fetched_at": entry.get("robots_fetched_at"),
                    "robots_http_status": entry.get("robots_http_status"),
                    "policy_scope": entry.get("policy_scope", "strict_multi"),
                    "policy_agents": self.policy_agents,
                    "robots_txt": entry.get("robots_txt"),
                    "error": entry.get("error"),
                }
            )
        return [{column: record.get(column) for column in ROBOTS_POLICY_COLUMNS} for record in records]

    # Loads one robots entry from memory, disk cache, or the network.
    def _get_or_fetch(self, domain: str) -> Dict[str, Any]:
        if domain in self._memory_cache and not self._is_stale(self._memory_cache[domain]):
            return self._memory_cache[domain]

        if self.cache_dir:
            cache_file = self._cache_file(domain)
            if cache_file.exists():
                entry = json.loads(cache_file.read_text())
                if not self._is_stale(entry):
                    self._memory_cache[domain] = entry
                    return entry

        entry = self._fetch_robots(domain)
        self._memory_cache[domain] = entry
        if self.cache_dir:
            self._cache_file(domain).write_text(json.dumps(entry, indent=2))
        return entry

    # Checks whether one cached robots entry should be refreshed.
    def _is_stale(self, entry: Dict[str, Any]) -> bool:
        fetched_at = entry.get("robots_fetched_at")
        if not fetched_at:
            return True
        try:
            fetched_dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        except Exception:
            return True
        age_seconds = (datetime.now(timezone.utc) - fetched_dt).total_seconds()
        return age_seconds > self.ttl_seconds

    # Fetches one domain robots.txt file and stores the raw policy content.
    def _fetch_robots(self, domain: str) -> Dict[str, Any]:
        robots_txt_url = f"https://{domain}/robots.txt"
        fetched_at = utcnow_iso()
        headers = {"User-Agent": self.user_agent}
        try:
            response = self.session.get(robots_txt_url, headers=headers, timeout=self.timeout)
            robots_txt = response.text if response.ok else None
            return {
                "robots_txt_url": robots_txt_url,
                "robots_fetched_at": fetched_at,
                "robots_http_status": response.status_code,
                "robots_txt": robots_txt,
                "policy_scope": "strict_multi",
                "error": None if response.ok or response.status_code == 404 else response.text[:500],
            }
        except requests.RequestException as exc:
            return {
                "robots_txt_url": robots_txt_url,
                "robots_fetched_at": fetched_at,
                "robots_http_status": None,
                "robots_txt": None,
                "policy_scope": "strict_multi",
                "error": str(exc),
            }

    # Builds a stable cache file path for one domain policy record.
    def _cache_file(self, domain: str) -> Path:
        digest = hashlib.sha256(domain.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    # Finds the longest matching allow/disallow prefix for one user-agent group.
    def _match_rule_prefix(self, robots_txt: str, path: str, agent: str) -> Tuple[Optional[str], Optional[bool]]:
        groups = self._parse_groups(robots_txt)
        rules = self._rules_for_agent(groups, agent)
        best_prefix = None
        best_allowed = None

        for directive, prefix in rules:
            candidate_prefix = prefix or "/"
            # The stored explanation should match the most specific rule prefix
            # that applies to the URL path, mirroring subtree inheritance.
            if candidate_prefix == "/" or path.startswith(candidate_prefix):
                if best_prefix is None or len(candidate_prefix) >= len(best_prefix):
                    best_prefix = candidate_prefix
                    best_allowed = directive == "allow"

        return best_prefix, best_allowed

    # Parses robots.txt into user-agent groups for rule explanation output.
    def _parse_groups(self, robots_txt: str) -> List[Dict[str, Any]]:
        groups: List[Dict[str, Any]] = []
        current_group: Optional[Dict[str, Any]] = None

        for raw_line in robots_txt.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue

            key, value = [part.strip() for part in line.split(":", 1)]
            key_lower = key.lower()
            if key_lower == "user-agent":
                if current_group is None or current_group["rules"]:
                    current_group = {"agents": [], "rules": []}
                    groups.append(current_group)
                current_group["agents"].append(value)
            elif key_lower in {"allow", "disallow"}:
                if current_group is None:
                    continue
                current_group["rules"].append((key_lower, value))

        return groups

    # Selects the specific or wildcard rules applicable to one user agent.
    def _rules_for_agent(self, groups: List[Dict[str, Any]], agent: str) -> List[Tuple[str, str]]:
        exact_rules: List[Tuple[str, str]] = []
        wildcard_rules: List[Tuple[str, str]] = []
        agent_lower = agent.lower()

        for group in groups:
            agents = [item.lower() for item in group["agents"]]
            if agent_lower in agents:
                exact_rules.extend(group["rules"])
            elif "*" in agents:
                wildcard_rules.extend(group["rules"])

        return exact_rules or wildcard_rules
