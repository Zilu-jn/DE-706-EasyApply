import pytest

from transform.normalize import (
    ats_job_key,
    content_hash,
    html_to_text,
    normalize_company,
    normalize_location,
    normalize_title,
    normalize_url,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Cloudflare, Inc.", "cloudflare"),
        ("IMC Trading LLC", "imc trading"),
        ("Johnson & Johnson", "johnson and johnson"),
        ("Société Générale", "societe generale"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_company(raw, expected):
    assert normalize_company(raw) == expected


def test_normalize_title_collapses_whitespace_and_dashes():
    assert normalize_title("  Data  Scientist — Intern ") == "data scientist - intern"


def test_normalize_location_aliases_dedupes_and_sorts():
    assert normalize_location("NYC / SF / New York") == "new york, ny / san francisco, ca"
    assert normalize_location(None) is None


@pytest.mark.parametrize(
    "a, b",
    [
        (
            "https://boards.greenhouse.io/figma/jobs/123?gh_src=abc",
            "https://job-boards.greenhouse.io/figma/jobs/123/",
        ),
        (
            "https://jobs.lever.co/tri/44812007-50a8-49e6-87ed-3fac9fa6b418/apply?lever-source=Simplify",
            "https://jobs.lever.co/tri/44812007-50a8-49e6-87ed-3fac9fa6b418",
        ),
        ("http://www.example.com/careers?utm_source=x&id=7", "https://example.com/careers?id=7"),
    ],
)
def test_normalize_url_treats_variants_as_equal(a, b):
    assert normalize_url(a) == normalize_url(b)


def test_normalize_url_keeps_meaningful_query_params():
    assert normalize_url("https://x.com/job?id=1") != normalize_url("https://x.com/job?id=2")


@pytest.mark.parametrize(
    "url, key",
    [
        ("https://job-boards.greenhouse.io/doximity/jobs/7592251", "greenhouse:7592251"),
        (
            "https://boards.greenhouse.io/cloudflare/jobs/7695702?gh_jid=7695702",
            "greenhouse:7695702",
        ),
        ("https://careers.example.com/jobs/?gh_jid=555", "greenhouse:555"),
        (
            "https://jobs.lever.co/TRI/44812007-50A8-49e6-87ed-3fac9fa6b418/apply",
            "lever:44812007-50a8-49e6-87ed-3fac9fa6b418",
        ),
        ("https://amat.wd1.myworkdayjobs.com/External/job/123", None),
        (None, None),
    ],
)
def test_ats_job_key(url, key):
    assert ats_job_key(url) == key


def test_html_to_text_keeps_inline_tags_in_sentence():
    html = "<p>We're hiring a <strong>Data Engineer</strong> to help.</p><ul><li>Python</li><li>SQL</li></ul>"
    assert html_to_text(html) == "We're hiring a Data Engineer to help.\n• Python\n• SQL"


def test_html_to_text_handles_empty():
    assert html_to_text(None) is None
    assert html_to_text("") is None


def test_content_hash_ignores_formatting_but_not_wording():
    base = content_hash("Data Intern", "NYC", "Must be authorized to work in the US.")
    reformatted = content_hash("data  intern", "nyc", "Must be authorized\n to work in the US. ")
    reworded = content_hash("Data Intern", "NYC", "Sponsorship is available.")
    assert base == reformatted
    assert base != reworded
