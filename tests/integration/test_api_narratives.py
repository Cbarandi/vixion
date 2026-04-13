"""Smoke API de lectura de narrativas (FastAPI + Postgres)."""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from vixion.api.deps import get_db_connection
from vixion.api.main import app
from vixion.contracts import RawIngestCandidate
from vixion.pipeline import process_item as pi

pytestmark = pytest.mark.integration


def _insert_test_source(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sources (source_kind, name, config)
            VALUES ('rss'::source_kind, 'test-src-api', '{}'::jsonb)
            RETURNING id
            """
        )
        return int(cur.fetchone()[0])


@pytest.fixture
def api_client(db_conn):
    def _override_db():
        yield db_conn

    app.dependency_overrides[get_db_connection] = _override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def test_health_reports_database(api_client):
    r = api_client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["database"] in ("ok", "error")


def test_narratives_list_and_detail(api_client, db_conn):
    sid = _insert_test_source(db_conn)
    cand = RawIngestCandidate(
        source_id=sid,
        title="API review narrative headline",
        body="Markets digest policy signals as liquidity conditions stabilize.",
        fetched_url=f"https://example.test/api/{sid}/story",
        native_id=f"api_test_{sid}_{uuid.uuid4().hex[:8]}",
        published_at=None,
    )
    res = pi.process_item(db_conn, cand)
    assert res.status == "completed"
    nid = str(res.narrative_id)

    r_list = api_client.get("/narratives", params={"limit": 20})
    assert r_list.status_code == 200
    data = r_list.json()
    assert "items" in data
    assert data["limit"] == 20
    assert data["offset"] == 0
    ids = {row["id"] for row in data["items"]}
    assert nid in ids

    r_top = api_client.get("/narratives/top", params={"limit": 5})
    assert r_top.status_code == 200
    top = r_top.json()
    assert "items" in top
    assert len(top["items"]) <= 5

    r_detail = api_client.get(f"/narratives/{nid}")
    assert r_detail.status_code == 200
    d = r_detail.json()
    assert d["narrative_id"] == nid
    assert d["current"]["title"]
    assert d["current"]["item_count"] >= 1
    assert isinstance(d["current"]["source_dist"], dict)
    assert len(d["events"]) >= 1
    assert len(d["snapshots"]) >= 1
    assert d["review"] is None

    r_404 = api_client.get(f"/narratives/{uuid.uuid4()}")
    assert r_404.status_code == 404

    r_items = api_client.get(f"/narratives/{nid}/items", params={"limit": 20})
    assert r_items.status_code == 200
    items_body = r_items.json()
    assert items_body["narrative_id"] == nid
    assert len(items_body["items"]) >= 1
    row0 = items_body["items"][0]
    assert "item_id" in row0
    assert "title" in row0
    assert "linked_at" in row0
    assert "url" in row0

    assert api_client.get(f"/narratives/{uuid.uuid4()}/items").status_code == 404

    r_post = api_client.post(
        f"/narratives/{nid}/reviews",
        json={
            "verdict": "good",
            "reason_code": "other",
            "notes": "checkpoint test",
            "reviewer": "pytest",
        },
    )
    assert r_post.status_code == 200
    pr = r_post.json()
    assert pr["review_id"] >= 1
    assert pr["narrative_id"] == nid

    r_detail2 = api_client.get(f"/narratives/{nid}")
    assert r_detail2.status_code == 200
    rev = r_detail2.json()["review"]
    assert rev is not None
    assert rev["verdict"] == "good"
    assert rev["reason_code"] == "other"

    ev = r_detail2.json()["events"]
    types = {e["event_type"] for e in ev}
    assert "REVIEW_RECORDED" in types


def test_narratives_filters(api_client, db_conn):
    sid = _insert_test_source(db_conn)
    cand = RawIngestCandidate(
        source_id=sid,
        title="Filter test item",
        body="English body for locale gate.",
        fetched_url=f"https://example.test/f/{sid}/x",
        native_id=f"api_filt_{sid}_{uuid.uuid4().hex[:8]}",
        published_at=None,
    )
    pi.process_item(db_conn, cand)

    r = api_client.get("/narratives", params={"min_score": 0, "include_dormant": True, "limit": 5})
    assert r.status_code == 200
    assert r.json()["items"]
