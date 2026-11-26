-- Count opportunities here to avoid counting the same opening once per source.
SELECT company, title, location, sources, apply_url
FROM easyapply.v_current_opportunities
WHERE is_target_role
ORDER BY company, title;

SELECT company, title, sources, posting_count
FROM easyapply.v_confirmed_duplicates
ORDER BY source_count DESC, company;

SELECT first_seen_at, company, title, apply_url
FROM easyapply.v_new_opportunities
ORDER BY first_seen_at DESC;

SELECT run_id, source, external_id, previous_title, title,
       content_hash <> previous_content_hash AS description_title_or_location_changed,
       extra IS DISTINCT FROM previous_extra AS source_metadata_changed
FROM easyapply.v_posting_changes
ORDER BY fetched_at DESC, source, external_id;

SELECT left_posting_id, right_posting_id, left_title, right_title, title_score, location_overlap
FROM easyapply.v_pending_reviews
ORDER BY title_score DESC;

SELECT student_key, decision, company, title, apply_url
FROM easyapply.v_student_follow_up
ORDER BY student_key, company;

SELECT feed_key, health_status, last_attempt_at, last_success_at, http_status
FROM easyapply.v_source_health
ORDER BY feed_key;

SELECT DISTINCT opportunity_key, company, title, value, excerpt, source, snapshot_path
FROM easyapply.v_current_requirement_evidence
WHERE attribute = 'skill' AND value = 'python'
ORDER BY company, title;

SELECT company, title, value, excerpt, evidence_field, method
FROM easyapply.v_current_requirement_evidence
WHERE attribute = 'sponsorship'
ORDER BY company, title;

SELECT company, count(*) AS unique_opportunities,
       count(*) FILTER (WHERE source_count > 1) AS found_in_multiple_sources
FROM easyapply.v_current_opportunities
WHERE is_target_role
GROUP BY company
ORDER BY unique_opportunities DESC, company;

SELECT location_normalized, count(*) AS unique_opportunities
FROM easyapply.v_current_opportunities
WHERE is_target_role
GROUP BY location_normalized
ORDER BY unique_opportunities DESC, location_normalized;
