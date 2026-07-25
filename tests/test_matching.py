"""Unit tests for title similarity baseline candidate matcher."""

from src.matching.baseline import compute_title_similarity


def test_compute_title_similarity_exact():
    t1 = "Will SpaceX land a starship on Mars in 2026?"
    t2 = "Will SpaceX land a Starship on Mars in 2026?"
    assert compute_title_similarity(t1, t2) == 1.0


def test_compute_title_similarity_reordered():
    t1 = "Rihanna album release before GTA VI"
    t2 = "New Rihanna Album before GTA VI?"
    score = compute_title_similarity(t1, t2)
    assert score > 0.75


def test_compute_title_similarity_unrelated():
    t1 = "Will Federal Reserve cut rates in September?"
    t2 = "Will Bitcoin reach $100k by December?"
    score = compute_title_similarity(t1, t2)
    assert score < 0.50
