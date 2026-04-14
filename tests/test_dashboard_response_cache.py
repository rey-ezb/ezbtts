from __future__ import annotations

import unittest

from web_dashboard.dashboard_response_cache import DashboardResponseCache, cache_key_for_params


class DashboardResponseCacheTests(unittest.TestCase):
    def test_cache_key_is_stable_for_same_params(self) -> None:
        params_a = {"start": ["2026-03-01"], "end": ["2026-03-31"], "sources": ["Sales,Samples"]}
        params_b = {"sources": ["Sales,Samples"], "end": ["2026-03-31"], "start": ["2026-03-01"]}
        self.assertEqual(cache_key_for_params(params_a), cache_key_for_params(params_b))

    def test_cache_returns_payload_when_signature_matches(self) -> None:
        cache = DashboardResponseCache()
        params = {"start": ["2026-03-01"]}
        payload = {"summary": {"start_date": "2026-03-01"}}
        cache.set(params, "sig-1", payload)
        self.assertEqual(cache.get(params, "sig-1"), payload)
        self.assertIsNone(cache.get(params, "sig-2"))


if __name__ == "__main__":
    unittest.main()
