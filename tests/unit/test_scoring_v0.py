from vixion.services import scoring_v0


def test_score_in_range():
    s, breakdown, pol = scoring_v0.score_narrative_v0(
        item_count=3,
        sentiment=0.2,
        intensity=0.5,
        distinct_sources=2,
    )
    assert 0 <= s <= 100
    assert pol
    assert "base_volume" in breakdown
    assert breakdown.get("scoring_layer") == "v0_placeholder_operational_only"
    assert breakdown.get("not_for_downstream_signal") is True
    assert "inputs" in breakdown


def test_state_from_score_monotonic():
    assert scoring_v0.state_from_score_v0(10) == "early"
    assert scoring_v0.state_from_score_v0(40) == "emerging"
    assert scoring_v0.state_from_score_v0(60) == "confirmed"
