import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.base import RunContext

FIXTURES = Path(__file__).parent / "fixtures"

HTTP_CFG = {"user_agent": "test", "timeout_seconds": 1, "max_retries": 1, "backoff_seconds": 0}
TARGET_KEYWORDS = [
    "data",
    "analyst",
    "machine learning",
    r"\bml\b",
    r"\bai\b",
    "scientist",
    "intern",
]


def load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def ctx(tmp_path):
    return RunContext(
        run_id="test-run",
        fetched_at=datetime(2026, 11, 19, 12, 0, tzinfo=UTC),
        data_dir=tmp_path,
    )


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    """Stands in for requests.Session. `responses` is a list consumed in order."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params})
        if not self.responses:
            raise AssertionError(f"Unexpected request to {url}")
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp
