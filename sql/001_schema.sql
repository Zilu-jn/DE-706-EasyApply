CREATE SCHEMA IF NOT EXISTS easyapply;

CREATE TABLE IF NOT EXISTS easyapply.schema_migrations (
    filename text PRIMARY KEY,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS easyapply.source_feeds (
    feed_key text PRIMARY KEY,
    source text NOT NULL CHECK (source IN ('greenhouse', 'lever', 'github')),
    feed_token text NOT NULL,
    display_name text NOT NULL,
    UNIQUE (source, feed_token),
    UNIQUE (feed_key, source)
);

CREATE TABLE IF NOT EXISTS easyapply.pipeline_runs (
    run_id text PRIMARY KEY,
    fetched_at timestamptz NOT NULL,
    batch_hash text NOT NULL CHECK (length(batch_hash) = 64),
    status text NOT NULL CHECK (status IN ('loading', 'loaded', 'failed')),
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at timestamptz,
    input_records integer NOT NULL CHECK (input_records >= 0),
    posting_count integer NOT NULL DEFAULT 0 CHECK (posting_count >= 0),
    review_count integer NOT NULL DEFAULT 0 CHECK (review_count >= 0),
    error_type text
);

CREATE TABLE IF NOT EXISTS easyapply.source_collections (
    run_id text NOT NULL REFERENCES easyapply.pipeline_runs,
    feed_key text NOT NULL REFERENCES easyapply.source_feeds,
    is_complete boolean NOT NULL,
    http_status integer CHECK (http_status BETWEEN 100 AND 599),
    expected_count integer CHECK (expected_count >= 0),
    received_count integer NOT NULL CHECK (received_count >= 0),
    snapshot_path text,
    checksum text CHECK (length(checksum) = 64),
    error text,
    PRIMARY KEY (run_id, feed_key)
);

CREATE TABLE IF NOT EXISTS easyapply.opportunities (
    opportunity_key text PRIMARY KEY CHECK (length(trim(opportunity_key)) > 0),
    opportunity_id text NOT NULL UNIQUE CHECK (length(opportunity_id) = 16)
);

CREATE TABLE IF NOT EXISTS easyapply.postings (
    posting_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source text NOT NULL,
    external_id text NOT NULL CHECK (length(trim(external_id)) > 0),
    feed_key text NOT NULL,
    opportunity_key text NOT NULL REFERENCES easyapply.opportunities,
    UNIQUE (source, external_id),
    UNIQUE (posting_id, feed_key),
    FOREIGN KEY (feed_key, source) REFERENCES easyapply.source_feeds (feed_key, source)
);

CREATE TABLE IF NOT EXISTS easyapply.posting_versions (
    version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    posting_id bigint NOT NULL REFERENCES easyapply.postings,
    state_hash text NOT NULL CHECK (length(state_hash) = 64),
    content_hash text NOT NULL CHECK (length(content_hash) = 64),
    company text NOT NULL,
    company_normalized text,
    title text NOT NULL,
    title_normalized text,
    location text,
    location_normalized text,
    apply_url text,
    apply_url_normalized text,
    ats_job_key text,
    description_text text,
    description_html text,
    is_target_role boolean NOT NULL,
    posted_at timestamptz,
    updated_at timestamptz,
    extra jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(extra) = 'object'),
    UNIQUE (posting_id, state_hash),
    UNIQUE (posting_id, version_id)
);

CREATE TABLE IF NOT EXISTS easyapply.posting_observations (
    run_id text NOT NULL,
    posting_id bigint NOT NULL,
    feed_key text NOT NULL,
    version_id bigint NOT NULL,
    snapshot_path text NOT NULL,
    source_updated_at timestamptz,
    source_extra jsonb NOT NULL CHECK (jsonb_typeof(source_extra) = 'object'),
    match_method text NOT NULL CHECK (match_method IN ('ats_id', 'url', 'none')),
    PRIMARY KEY (run_id, posting_id),
    FOREIGN KEY (run_id, feed_key) REFERENCES easyapply.source_collections,
    FOREIGN KEY (posting_id, feed_key) REFERENCES easyapply.postings (posting_id, feed_key),
    FOREIGN KEY (posting_id, version_id)
        REFERENCES easyapply.posting_versions (posting_id, version_id)
);

CREATE TABLE IF NOT EXISTS easyapply.review_candidates (
    left_posting_id bigint NOT NULL REFERENCES easyapply.postings,
    right_posting_id bigint NOT NULL REFERENCES easyapply.postings,
    first_seen_run text NOT NULL REFERENCES easyapply.pipeline_runs,
    last_seen_run text NOT NULL REFERENCES easyapply.pipeline_runs,
    title_score double precision NOT NULL CHECK (title_score BETWEEN 0 AND 100),
    location_overlap boolean NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed_duplicate', 'distinct', 'dismissed')),
    reviewer text,
    reviewed_at timestamptz,
    notes text,
    PRIMARY KEY (left_posting_id, right_posting_id),
    CHECK (left_posting_id < right_posting_id)
);

CREATE TABLE IF NOT EXISTS easyapply.requirement_evidence (
    evidence_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version_id bigint NOT NULL REFERENCES easyapply.posting_versions,
    attribute text NOT NULL CHECK (attribute IN ('skill', 'sponsorship')),
    value text NOT NULL,
    excerpt text NOT NULL CHECK (length(trim(excerpt)) > 0),
    evidence_field text NOT NULL CHECK (evidence_field IN ('description_text', 'extra.sponsorship')),
    method text NOT NULL,
    UNIQUE (version_id, attribute, value, excerpt, method)
);

CREATE TABLE IF NOT EXISTS easyapply.student_decisions (
    student_key text NOT NULL CHECK (length(trim(student_key)) > 0),
    opportunity_key text NOT NULL REFERENCES easyapply.opportunities,
    decision text NOT NULL CHECK (decision IN ('saved', 'applied', 'dismissed', 'reviewed')),
    reviewed_signature text NOT NULL,
    decided_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes text,
    PRIMARY KEY (student_key, opportunity_key)
);

CREATE INDEX IF NOT EXISTS postings_opportunity_idx ON easyapply.postings (opportunity_key);
CREATE INDEX IF NOT EXISTS postings_feed_idx ON easyapply.postings (feed_key);
CREATE INDEX IF NOT EXISTS observations_posting_idx
    ON easyapply.posting_observations (posting_id, run_id);
CREATE INDEX IF NOT EXISTS observations_version_idx
    ON easyapply.posting_observations (version_id);
CREATE INDEX IF NOT EXISTS runs_fetched_idx ON easyapply.pipeline_runs (fetched_at DESC, run_id);
CREATE INDEX IF NOT EXISTS collections_feed_idx
    ON easyapply.source_collections (feed_key, run_id) WHERE is_complete;
CREATE INDEX IF NOT EXISTS versions_company_idx ON easyapply.posting_versions (company_normalized);
CREATE INDEX IF NOT EXISTS versions_location_idx ON easyapply.posting_versions (location_normalized);
CREATE INDEX IF NOT EXISTS evidence_lookup_idx
    ON easyapply.requirement_evidence (attribute, value, version_id);
CREATE INDEX IF NOT EXISTS reviews_pending_idx
    ON easyapply.review_candidates (title_score DESC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS decisions_opportunity_idx
    ON easyapply.student_decisions (opportunity_key);
