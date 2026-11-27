"""Duplicate-detection accuracy on the manually labeled sample (evaluation/labeled_pairs.csv).

Guards the quality bar, not just the plumbing: no false merges, and every
duplicate the matcher misses must at least reach the human review queue.
"""

from evaluation.dedup_accuracy import evaluate_on_fixtures, load_labeled_pairs


def test_labeled_sample_has_both_classes():
    labels = {row["label"] for row in load_labeled_pairs()}
    assert labels == {"duplicate", "distinct"}


def test_no_incorrect_matches_on_labeled_sample():
    report = evaluate_on_fixtures()
    assert report["incorrect_matches"] == []
    assert report["precision"] == 1.0


def test_missed_duplicates_are_caught_by_review_queue():
    report = evaluate_on_fixtures()
    # The TRI repost (different Lever IDs) is expected to be missed by exact
    # matching, but it must land in the review queue rather than vanish.
    assert all(m["in_review_queue"] for m in report["missed_duplicates"])
    assert report["recall"] >= 0.6
    assert report["review_adjusted_recall"] == 1.0
