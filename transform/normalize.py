"""Cleaning and standardization (Stage 1 -> Stage 2 columns).

Small pure functions do the string work so they are easy to unit test;
`normalize_frame` applies them to a Polars DataFrame. Original columns are
always kept next to the normalized ones.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import polars as pl
from bs4 import BeautifulSoup

# ---------------------------------------------------------------- companies

_LEGAL_SUFFIXES = {
    "inc",
    "incorporated",
    "llc",
    "ltd",
    "limited",
    "corp",
    "corporation",
    "co",
    "company",
    "plc",
    "lp",
    "llp",
    "gmbh",
    "sa",
    "ag",
}


def normalize_company(name: str | None) -> str | None:
    if not name:
        return None
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    words = s.split()
    while words and words[-1] in _LEGAL_SUFFIXES:
        words.pop()
    return " ".join(words) or None


# ---------------------------------------------------------------- titles


def normalize_title(title: str | None) -> str | None:
    if not title:
        return None
    s = unicodedata.normalize("NFKC", title).lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip(" -,")
    return s or None


# ---------------------------------------------------------------- locations

_LOCATION_ALIASES = {
    "sf": "san francisco, ca",
    "san francisco": "san francisco, ca",
    "nyc": "new york, ny",
    "new york city": "new york, ny",
    "new york": "new york, ny",
    "la": "los angeles, ca",
    "dc": "washington, dc",
    "remote in usa": "remote - us",
    "remote in us": "remote - us",
    "remote, us": "remote - us",
    "remote - usa": "remote - us",
    "us remote": "remote - us",
    "united states (remote)": "remote - us",
}


def normalize_location(location: str | None) -> str | None:
    """Lowercase each part of a ' / '-joined location string and expand common aliases."""
    if not location:
        return None
    parts = []
    for part in location.split(" / "):
        p = re.sub(r"\s+", " ", part.strip().lower())
        p = _LOCATION_ALIASES.get(p, p)
        if p and p not in parts:
            parts.append(p)
    return " / ".join(sorted(parts)) or None


# ---------------------------------------------------------------- URLs

_TRACKING_PARAMS = {
    "gh_src",
    "source",
    "src",
    "ref",
    "referrer",
    "lever-source",
    "lever-origin",
    "lever-source[]",
    "utm",
    "t",
    "s",
    "trk",
    "jobsource",
}
_GREENHOUSE_HOSTS = {
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "job-boards.eu.greenhouse.io",
}
_LEVER_HOSTS = {"jobs.lever.co", "jobs.eu.lever.co"}


def normalize_url(url: str | None) -> str | None:
    """Canonical form of an application URL, so the same link written two ways compares equal."""
    if not url or not url.strip():
        return None
    parts = urlsplit(url.strip())
    host = parts.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if host in _GREENHOUSE_HOSTS:
        host = "job-boards.greenhouse.io"
    if host in _LEVER_HOSTS:
        host = "jobs.lever.co"
    path = re.sub(r"/+", "/", parts.path).rstrip("/")
    if host == "jobs.lever.co" and path.endswith("/apply"):
        path = path[: -len("/apply")]
    query = sorted(
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=False)
        if not k.lower().startswith("utm_") and k.lower() not in _TRACKING_PARAMS
    )
    return urlunsplit(("https", host, path.lower(), urlencode(query), ""))


_GH_PATH = re.compile(r"greenhouse\.io/[^/]+/jobs/(\d+)")
_GH_QUERY = re.compile(r"[?&](?:gh_jid|token)=(\d+)")
_LEVER_PATH = re.compile(
    r"lever\.co/[^/]+/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I
)


def ats_job_key(url: str | None) -> str | None:
    """Pull the applicant-tracking-system job ID out of a URL.

    Greenhouse job IDs and Lever posting UUIDs are globally unique, so two
    records with the same key are the same posting even if they came from
    different sources or the URL was written differently.
    """
    if not url:
        return None
    if m := _LEVER_PATH.search(url):
        return f"lever:{m.group(1).lower()}"
    if "greenhouse.io" in url or "gh_jid=" in url:
        if m := _GH_PATH.search(url) or _GH_QUERY.search(url):
            return f"greenhouse:{m.group(1)}"
    return None


# ---------------------------------------------------------------- text

_BLOCK_TAGS = ["p", "div", "br", "li", "ul", "ol", "tr", "h1", "h2", "h3", "h4", "h5", "h6"]


def html_to_text(html_str: str | None) -> str | None:
    """Plain text with one line per block element. Inline tags (<strong>, <a>)
    stay inside their sentence, which matters for sentence splitting later."""
    if not html_str:
        return None
    soup = BeautifulSoup(html_str, "html.parser")
    for tag in soup.find_all(_BLOCK_TAGS):
        if tag.name == "li":
            tag.insert_before("\n• ")
        tag.insert_after("\n")
    text = soup.get_text().replace("\xa0", " ")
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in text.splitlines()]
    text = "\n".join(ln for ln in lines if ln and ln != "•")
    text = re.sub(r"•\n", "• ", text)
    return text or None


def content_hash(title: str | None, location: str | None, description: str | None) -> str:
    """Hash of the fields that define a posting's content.

    Whitespace and case are normalized first, so re-formatting alone doesn't
    look like a new version, but any real wording change does.
    """

    def clean(s: str | None) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip().lower()

    payload = "\x1f".join([clean(title), clean(location), clean(description)])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- frame


def _map(series: pl.Series, fn) -> pl.Series:
    return pl.Series(series.name, [fn(v) for v in series.to_list()], dtype=pl.Utf8)


def normalize_frame(df: pl.DataFrame, target_role_keywords: list[str]) -> pl.DataFrame:
    """Add the Stage 2 cleaned columns to a raw_records frame."""
    pattern = "(?i)" + "|".join(f"(?:{k})" for k in target_role_keywords)
    desc_text = _map(df["description_html"], html_to_text).alias("description_text")
    out = df.with_columns(
        _map(df["company"], normalize_company).alias("company_normalized"),
        _map(df["title"], normalize_title).alias("title_normalized"),
        _map(df["location"], normalize_location).alias("location_normalized"),
        _map(df["apply_url"], normalize_url).alias("apply_url_normalized"),
        _map(df["apply_url"], ats_job_key).alias("ats_job_key"),
        desc_text,
    )
    hashes = [
        content_hash(t, loc, d)
        for t, loc, d in zip(
            out["title"].to_list(),
            out["location"].to_list(),
            out["description_text"].to_list(),
            strict=True,
        )
    ]
    return out.with_columns(
        pl.Series("content_hash", hashes, dtype=pl.Utf8),
        pl.col("title").fill_null("").str.contains(pattern).alias("is_target_role"),
    )
