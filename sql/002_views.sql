CREATE OR REPLACE VIEW easyapply.v_published_observations AS
SELECT o.*, r.fetched_at
FROM easyapply.posting_observations o
JOIN easyapply.pipeline_runs r USING (run_id)
JOIN easyapply.source_collections c USING (run_id, feed_key)
WHERE r.status = 'loaded' AND c.is_complete;

CREATE OR REPLACE VIEW easyapply.v_latest_complete_collections AS
SELECT DISTINCT ON (c.feed_key) c.*, r.fetched_at
FROM easyapply.source_collections c
JOIN easyapply.pipeline_runs r USING (run_id)
WHERE c.is_complete AND r.status = 'loaded'
ORDER BY c.feed_key, r.fetched_at DESC, r.run_id DESC;

CREATE OR REPLACE VIEW easyapply.v_current_postings AS
WITH latest AS (
    SELECT DISTINCT ON (posting_id) *
    FROM easyapply.v_published_observations
    ORDER BY posting_id, fetched_at DESC, run_id DESC
)
SELECT p.source, p.external_id, p.feed_key, p.opportunity_key,
       v.*, o.run_id, o.fetched_at AS last_seen_at, o.snapshot_path, o.source_updated_at,
       o.source_extra, o.match_method,
       o.run_id = c.run_id AS is_present,
       c.fetched_at AS feed_last_success_at
FROM latest o
JOIN easyapply.postings p USING (posting_id)
JOIN easyapply.posting_versions v USING (posting_id, version_id)
JOIN easyapply.v_latest_complete_collections c ON c.feed_key = p.feed_key;

CREATE OR REPLACE VIEW easyapply.v_opportunity_discovery AS
SELECT DISTINCT ON (p.opportunity_key)
       p.opportunity_key, o.run_id AS first_seen_run, o.fetched_at AS first_seen_at
FROM easyapply.postings p
JOIN easyapply.v_published_observations o USING (posting_id)
ORDER BY p.opportunity_key, o.fetched_at, o.run_id;

CREATE OR REPLACE VIEW easyapply.v_current_opportunities AS
WITH ranked AS (
    SELECT c.*, row_number() OVER (
        PARTITION BY opportunity_key
        ORDER BY (source <> 'github') DESC, (description_text IS NOT NULL) DESC,
                 last_seen_at DESC, source, external_id
    ) AS preference
    FROM easyapply.v_current_postings c WHERE is_present
), coverage AS (
    SELECT opportunity_key, count(*) AS posting_count, count(DISTINCT source) AS source_count,
           array_agg(DISTINCT source ORDER BY source) AS sources,
           bool_or(is_target_role) AS is_target_role,
           md5(string_agg(posting_id::text || ':' || state_hash, ',' ORDER BY posting_id))
               AS current_signature
    FROM ranked GROUP BY opportunity_key
)
SELECT p.opportunity_key, k.opportunity_id, p.company, p.company_normalized,
       p.title, p.location, p.location_normalized, p.apply_url,
       p.description_text, p.posted_at, c.is_target_role,
       c.posting_count, c.source_count, c.sources, c.current_signature,
       d.first_seen_at, d.first_seen_run, p.last_seen_at
FROM ranked p
JOIN coverage c USING (opportunity_key)
JOIN easyapply.opportunities k USING (opportunity_key)
JOIN easyapply.v_opportunity_discovery d USING (opportunity_key)
WHERE p.preference = 1;

CREATE OR REPLACE VIEW easyapply.v_confirmed_duplicates AS
SELECT opportunity_key, opportunity_id, company, title, sources, posting_count, source_count
FROM easyapply.v_current_opportunities WHERE source_count > 1;

CREATE OR REPLACE VIEW easyapply.v_new_opportunities AS
SELECT * FROM easyapply.v_current_opportunities
WHERE first_seen_at >= CURRENT_TIMESTAMP - interval '7 days';

CREATE OR REPLACE VIEW easyapply.v_posting_changes AS
WITH history AS (
    SELECT p.source, p.external_id, p.opportunity_key, o.run_id, o.fetched_at,
           o.snapshot_path, v.version_id, v.state_hash, v.content_hash,
           v.title, v.location, v.extra,
           lag(v.state_hash) OVER w AS previous_state_hash,
           lag(v.content_hash) OVER w AS previous_content_hash,
           lag(v.title) OVER w AS previous_title,
           lag(v.location) OVER w AS previous_location,
           lag(v.extra) OVER w AS previous_extra
    FROM easyapply.v_published_observations o
    JOIN easyapply.postings p USING (posting_id)
    JOIN easyapply.posting_versions v USING (posting_id, version_id)
    WINDOW w AS (PARTITION BY p.posting_id ORDER BY o.fetched_at, o.run_id)
)
SELECT * FROM history
WHERE previous_state_hash IS NOT NULL AND state_hash <> previous_state_hash;

CREATE OR REPLACE VIEW easyapply.v_pending_reviews AS
SELECT c.*, l.source AS left_source, l.external_id AS left_external_id,
       lv.title AS left_title, lv.location AS left_location,
       r.source AS right_source, r.external_id AS right_external_id,
       rv.title AS right_title, rv.location AS right_location
FROM easyapply.review_candidates c
JOIN easyapply.postings l ON l.posting_id = c.left_posting_id
JOIN easyapply.postings r ON r.posting_id = c.right_posting_id
JOIN easyapply.v_current_postings lv ON lv.posting_id = l.posting_id
JOIN easyapply.v_current_postings rv ON rv.posting_id = r.posting_id
WHERE c.status = 'pending' AND lv.is_present AND rv.is_present;

CREATE OR REPLACE VIEW easyapply.v_current_requirement_evidence AS
SELECT p.opportunity_key, p.source, p.external_id, p.company, p.title,
       e.*, p.run_id, p.snapshot_path
FROM easyapply.requirement_evidence e
JOIN easyapply.v_current_postings p USING (version_id)
WHERE p.is_present;

CREATE OR REPLACE VIEW easyapply.v_student_follow_up AS
SELECT d.student_key, d.decision, d.decided_at, d.notes, c.*
FROM easyapply.student_decisions d
JOIN easyapply.v_current_opportunities c USING (opportunity_key)
WHERE d.decision <> 'dismissed' AND d.reviewed_signature <> c.current_signature;

CREATE OR REPLACE VIEW easyapply.v_source_health AS
WITH attempts AS (
    SELECT DISTINCT ON (c.feed_key) c.*, r.fetched_at, r.status AS load_status
    FROM easyapply.source_collections c
    JOIN easyapply.pipeline_runs r USING (run_id)
    ORDER BY c.feed_key, r.fetched_at DESC, r.run_id DESC
)
SELECT f.*, a.run_id AS latest_attempt_run, a.fetched_at AS last_attempt_at,
       a.is_complete AS latest_complete, a.http_status, a.error, a.load_status,
       s.run_id AS last_success_run, s.fetched_at AS last_success_at,
       CURRENT_TIMESTAMP - s.fetched_at AS success_age,
       CASE WHEN a.run_id IS NULL THEN 'never_collected'
            WHEN a.load_status = 'loading' THEN 'loading'
            WHEN a.load_status <> 'loaded' THEN 'load_failed'
            WHEN NOT a.is_complete THEN 'incomplete'
            WHEN s.fetched_at < CURRENT_TIMESTAMP - interval '24 hours' THEN 'stale'
            ELSE 'healthy' END AS health_status
FROM easyapply.source_feeds f
LEFT JOIN attempts a USING (feed_key)
LEFT JOIN easyapply.v_latest_complete_collections s USING (feed_key);
