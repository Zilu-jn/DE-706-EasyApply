"""Shared pieces for all source adapters: the record shape, run context,
HTTP with retries, and raw snapshot storage."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
import yaml

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"


def load_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class RunContext:
    """Identifies one collection run. Every record and snapshot carries its run_id."""

    run_id: str
    fetched_at: datetime
    data_dir: Path

    @classmethod
    def new(cls, data_dir: Path | str = "data") -> RunContext:
        now = datetime.now(UTC)
        run_id = now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
        return cls(run_id=run_id, fetched_at=now, data_dir=Path(data_dir))


@dataclass
class RawRecord:
    """One posting as collected, before cleaning. See docs/data_contract.md."""

    source: str
    external_id: str
    company_token: str | None
    company: str
    title: str
    location: str | None
    apply_url: str | None
    description_html: str | None
    posted_at: datetime | None
    updated_at: datetime | None
    fetched_at: datetime
    run_id: str
    snapshot_path: str
    extra: str = "{}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CollectionResult:
    """Outcome of collecting one source/company. Goes into the run manifest."""

    source: str
    company_token: str | None
    records: list[RawRecord] = field(default_factory=list)
    snapshot_path: str | None = None
    checksum: str | None = None
    http_status: int | None = None
    expected_count: int | None = None
    is_complete: bool = False
    error: str | None = None

    def manifest_entry(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "company_token": self.company_token,
            "snapshot_path": self.snapshot_path,
            "checksum": self.checksum,
            "http_status": self.http_status,
            "expected_count": self.expected_count,
            "received_count": len(self.records),
            "is_complete": self.is_complete,
            "error": self.error,
        }


class FetchError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def http_get_json(
    url: str,
    *,
    session: requests.Session | None = None,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30,
    max_retries: int = 3,
    backoff: float = 2,
) -> tuple[Any, int]:
    """GET a JSON endpoint, retrying on timeouts, 429 and 5xx. Returns (data, status)."""
    sess = session or requests.Session()
    last_error: str = ""
    last_status: int | None = None
    for attempt in range(max_retries + 1):
        try:
            resp = sess.get(url, params=params, headers=headers, timeout=timeout)
            last_status = resp.status_code
            if resp.status_code == 200:
                return resp.json(), 200
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = f"HTTP {resp.status_code}"
            else:
                # 4xx other than 429 won't fix itself; fail fast
                raise FetchError(f"HTTP {resp.status_code} for {url}", resp.status_code)
        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = f"{type(e).__name__}: {e}"
        except ValueError as e:  # invalid JSON
            raise FetchError(f"Invalid JSON from {url}: {e}", last_status) from e
        if attempt < max_retries:
            time.sleep(backoff * (2**attempt))
    raise FetchError(f"Gave up on {url} after {max_retries + 1} tries: {last_error}", last_status)


def save_snapshot(payload: Any, source: str, key: str, ctx: RunContext) -> tuple[str, str]:
    """Write the raw response to data/raw/{source}/{date}/{run_id}/{key}.json.

    Returns (path, sha256 checksum). The checksum covers the exact bytes written,
    so later steps can prove which raw file a record came from.
    """
    day = ctx.fetched_at.strftime("%Y-%m-%d")
    out_dir = ctx.data_dir / "raw" / source / day / ctx.run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{key}.json"
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    path.write_bytes(body)
    return str(path), hashlib.sha256(body).hexdigest()


def parse_datetime(value: Any) -> datetime | None:
    """Parse ISO-8601 strings or epoch seconds/milliseconds into UTC datetimes."""
    if value is None or value == "":
        return None
    if isinstance(value, int | float) or (isinstance(value, str) and value.isdigit()):
        num = float(value)
        if num > 1e11:  # milliseconds
            num /= 1000
        return datetime.fromtimestamp(num, tz=UTC)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
