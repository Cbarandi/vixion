from vixion.services import canonicalization


def test_normalize_url_strips_utm():
    u = canonicalization.normalize_url(
        "https://Example.com/path/?utm_source=x&id=1&utm_medium=email"
    )
    assert "utm_" not in (u or "")
    assert u and "example.com" in u.lower()


def test_content_hash_stable_and_length():
    h = canonicalization.content_hash("T", "body")
    h2 = canonicalization.content_hash("T", "body")
    assert h == h2
    assert len(h) >= 32


def test_occurrence_fingerprint_stable():
    a = canonicalization.occurrence_fingerprint(1, "nid", "https://a", "hash")
    b = canonicalization.occurrence_fingerprint(1, "nid", "https://a", "hash")
    assert a == b
