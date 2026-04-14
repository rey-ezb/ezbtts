from __future__ import annotations

import json
from typing import Any


def cache_key_for_params(params: dict[str, list[str]]) -> str:
    normalized = {str(key): [str(value) for value in values] for key, values in sorted(params.items())}
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


class DashboardResponseCache:
    def __init__(self) -> None:
        self._entries: dict[str, dict[str, Any]] = {}

    def get(self, params: dict[str, list[str]], signature: str) -> dict[str, Any] | None:
        entry = self._entries.get(cache_key_for_params(params))
        if entry and entry.get("signature") == signature:
            return entry.get("payload")
        return None

    def set(self, params: dict[str, list[str]], signature: str, payload: dict[str, Any]) -> None:
        self._entries[cache_key_for_params(params)] = {"signature": signature, "payload": payload}

    def clear(self) -> None:
        self._entries.clear()
