"""Checks for the two spike improvements: the report queue and the report cache.

Needs two running PostgreSQL databases with the schema from db/init.sql:
  shared DB (small tenants)    TEST_SHARED_DSN, default postgresql://erp:erp@localhost:5432/erp
  whale DB  (tenant 1 only)    TEST_WHALE_DSN,  default postgresql://erp:erp@localhost:5433/erp
Seed them first:
  DATABASE_URL=<shared> python -m app.seed
  DATABASE_URL=<whale> SMALL_TENANTS=1 SMALL_ORDERS=1 python -m app.seed
and remove the whale's rows from the shared DB (see README, "Running the load test and the automatic checks").

Run:  pytest -v tests/
Each test starts its own API process with the settings it needs.
"""
import json
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import httpx
import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[1]
SHARED = os.getenv("TEST_SHARED_DSN", "postgresql://erp:erp@localhost:5432/erp")
WHALE = os.getenv("TEST_WHALE_DSN", "postgresql://erp:erp@localhost:5433/erp")
WHALE_ID, SMALL_ID = 1, 2
REPORT = f"/t/{WHALE_ID}/reports/monthly"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def api(**settings):
    """Start the API with the given env settings; the whale is routed to its own DB."""
    port = _free_port()
    env = {**os.environ, "DATABASE_URL": SHARED, "POOL_MAX": "10",
           "TENANT_DSN_MAP": json.dumps({str(WHALE_ID): WHALE}),
           **{k: str(v) for k, v in settings.items()}}
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=60) as client:
            for _ in range(80):
                try:
                    if client.get("/health").status_code == 200:
                        break
                except httpx.HTTPError:
                    time.sleep(0.25)
            else:
                raise RuntimeError("API did not start")
            yield client
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:  # e.g. a DB pool still closing after whale add
            proc.kill()
            proc.wait(10)


def parallel(client, n, path=REPORT):
    """Send n requests at the same time; return their responses."""
    with ThreadPoolExecutor(n) as pool:
        return list(pool.map(lambda _: client.get(path), range(n)))


class RunningReports:
    """Samples how many month-end report queries run on the whale DB at the same time."""

    def __init__(self):
        self.max_seen = 0
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        with psycopg.connect(WHALE, autocommit=True) as conn:
            while not self._stop.is_set():
                n = conn.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' "
                    "AND backend_type = 'client backend' AND query LIKE '%%date_trunc%%' "
                    "AND pid <> pg_backend_pid()").fetchone()[0]
                self.max_seen = max(self.max_seen, n)
                time.sleep(0.002)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join()


def count(dsn, sql, *args):
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql, args).fetchone()[0]


# --- data isolation (AC1) ----------------------------------------------------

def test_shared_db_holds_no_whale_rows():
    assert count(SHARED, "SELECT count(*) FROM orders WHERE tenant_id = %s", WHALE_ID) == 0
    assert count(SHARED, "SELECT count(*) FROM order_lines WHERE tenant_id = %s", WHALE_ID) == 0
    assert count(WHALE, "SELECT count(*) FROM orders WHERE tenant_id = %s", WHALE_ID) > 0


def test_new_whale_order_lands_in_whale_db_only():
    before_whale = count(WHALE, "SELECT count(*) FROM orders WHERE tenant_id = %s", WHALE_ID)
    with api() as c:
        r = c.post(f"/t/{WHALE_ID}/orders",
                   json={"lines": [{"product": "SKU-ISOLATION-TEST", "qty": 1, "unit_price": "1.00"}]})
        assert r.status_code == 201
    try:
        assert count(WHALE, "SELECT count(*) FROM orders WHERE tenant_id = %s", WHALE_ID) == before_whale + 1
        assert count(SHARED, "SELECT count(*) FROM orders WHERE tenant_id = %s", WHALE_ID) == 0
    finally:
        with psycopg.connect(WHALE, autocommit=True) as conn:
            conn.execute("DELETE FROM order_lines WHERE product = 'SKU-ISOLATION-TEST'")
            conn.execute("DELETE FROM orders WHERE id = %s", (r.json()["id"],))


# --- improvement 1: report queue (AC4) ----------------------------------------

def test_settings_off_keeps_old_behaviour():
    with api() as c, RunningReports() as seen:
        responses = parallel(c, 8)
    assert all(r.status_code == 200 for r in responses)
    assert seen.max_seen > 2, "without the queue, more than 2 reports should run at once"


def test_queue_runs_at_most_two_reports_and_rejects_none():
    with api(REPORT_CONCURRENCY=2) as c:
        with RunningReports() as seen:
            responses = parallel(c, 12)
        metrics = c.get("/admin/metrics").json()
    assert [r.status_code for r in responses] == [200] * 12
    assert 1 <= seen.max_seen <= 2
    assert metrics["report_rejections"] == {}
    assert metrics["settings"]["report_concurrency"] == 2


def test_queue_returns_429_only_after_long_wait():
    with api(REPORT_CONCURRENCY=1, REPORT_QUEUE_TIMEOUT=0.05) as c:
        responses = parallel(c, 8)
        metrics = c.get("/admin/metrics").json()
    codes = [r.status_code for r in responses]
    assert 200 in codes and 429 in codes
    assert all(code in (200, 429) for code in codes)
    rejected = [r for r in responses if r.status_code == 429]
    assert all("retry later" in r.json()["detail"] for r in rejected)
    assert metrics["report_rejections"] == {str(WHALE_ID): len(rejected)}


def test_rate_cap_spreads_report_starts():
    with api(REPORT_RATE=5) as c:
        t0 = time.perf_counter()
        responses = parallel(c, 10)
        elapsed = time.perf_counter() - t0
    assert all(r.status_code == 200 for r in responses)
    assert elapsed >= 1.7, f"10 reports at 5/s must take at least ~1.8 s, took {elapsed:.2f}"


def test_light_requests_are_never_queued():
    with api(REPORT_CONCURRENCY=1) as c:
        with ThreadPoolExecutor(13) as pool:
            reports = [pool.submit(c.get, REPORT) for _ in range(12)]
            time.sleep(0.2)  # reports are now running or waiting in line
            t0 = time.perf_counter()
            light = c.get(f"/t/{WHALE_ID}/orders", params={"limit": 5})
            small = c.get(f"/t/{SMALL_ID}/orders", params={"limit": 5})
            light_ms = (time.perf_counter() - t0) * 1000
            assert all(f.result().status_code == 200 for f in reports)
    assert light.status_code == 200 and small.status_code == 200
    assert light_ms < 1000, f"light requests waited behind reports ({light_ms:.0f} ms)"


def test_queue_does_not_touch_other_tenants():
    with api(REPORT_CONCURRENCY=1, REPORT_QUEUE_TIMEOUT=0.05) as c:
        with ThreadPoolExecutor(9) as pool:
            whale = [pool.submit(c.get, REPORT) for _ in range(8)]
            small = pool.submit(c.get, f"/t/{SMALL_ID}/reports/monthly")
            assert small.result().status_code == 200
            [f.result() for f in whale]


# --- improvement 2: report cache ----------------------------------------------

def test_cached_report_equals_fresh_report():
    with api() as c:
        fresh = c.get(REPORT).json()
    with api(REPORT_CACHE_SECONDS=60) as c:
        first, second = c.get(REPORT), c.get(REPORT)
    assert first.status_code == second.status_code == 200
    assert first.json() == fresh and second.json() == fresh
    assert len(fresh) == 200


def test_cache_is_per_tenant():
    with api(REPORT_CACHE_SECONDS=60) as c:
        whale = c.get(REPORT).json()
        small = c.get(f"/t/{SMALL_ID}/reports/monthly").json()
    assert whale != small


def test_concurrent_requests_compute_report_once():
    with api(REPORT_CACHE_SECONDS=60) as c, RunningReports() as seen:
        responses = parallel(c, 15)
    assert all(r.status_code == 200 for r in responses)
    assert len({r.content for r in responses}) == 1
    assert seen.max_seen <= 1, "only one request should calculate the report"


def test_cache_expires_and_shows_new_data():
    bump = ("UPDATE order_lines l SET qty = qty + %s FROM orders o WHERE o.id = l.order_id "
            "AND o.tenant_id = 1 AND l.product = %s "
            "AND o.order_date >= '2026-01-01' AND o.order_date < '2026-02-01'")
    with api(REPORT_CACHE_SECONDS=1.5) as c:
        before = c.get(REPORT).json()
        product = before[0]["product"]
        with psycopg.connect(WHALE, autocommit=True) as conn:
            conn.execute(bump, (1000, product))
        try:
            still_cached = c.get(REPORT).json()
            time.sleep(1.7)
            after = c.get(REPORT).json()
        finally:
            with psycopg.connect(WHALE, autocommit=True) as conn:
                conn.execute(bump, (-1000, product))
    assert still_cached == before
    units = lambda rows: next(r["units"] for r in rows if r["product"] == product and r["month"] == "2026-01-01")
    assert units(after) > units(before)


def test_removing_whale_clears_its_cached_report():
    with api(REPORT_CACHE_SECONDS=60) as c:
        assert len(c.get(REPORT).json()) > 0  # report is now in the cache
        try:
            assert c.post("/admin/whale/remove").json()["present"] is False
            assert c.get(REPORT).json() == [], "old cached report was returned after removal"
        finally:
            # put the whale data back (faster than /admin/whale/add, same data)
            subprocess.run([sys.executable, "-m", "app.seed"], cwd=ROOT, check=True,
                           env={**os.environ, "DATABASE_URL": WHALE,
                                "SMALL_TENANTS": "1", "SMALL_ORDERS": "1"})
