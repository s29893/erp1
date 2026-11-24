"""Mini-ERP, multi-tenant, shared stack. Deliberately simple: a base for spikes.

Tenant comes from the URL (/t/{tenant_id}/...) to keep the code readable.
In a real system it would come from the auth token, never from the client.
"""
import json
import os
import threading
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db, guardrail, simulation, whale

app = FastAPI(title="Mini-ERP multi-tenant base (spike starter)")


@app.middleware("http")
async def measure(request: Request, call_next):
    tenant_id = guardrail.tenant_from_path(request.url.path)
    start = time.perf_counter()
    response = await call_next(request)
    if tenant_id is not None:
        guardrail.record(tenant_id, (time.perf_counter() - start) * 1000, response.status_code)
    return response


class Line(BaseModel):
    product: str
    qty: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)


class OrderIn(BaseModel):
    lines: list[Line] = Field(min_length=1)


STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def page_client():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/ops", include_in_schema=False)
def page_ops():
    return FileResponse(STATIC_DIR / "ops.html")


@app.get("/api/tenants")
def list_tenants():
    """Tenant list for the UI dropdown: the whale plus the first small tenants."""
    with db.tenant_conn(guardrail.WHALE_ID) as conn:
        return conn.execute(
            "SELECT id, name, plan FROM tenants WHERE id <= 20 ORDER BY id"
        ).fetchall()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/t/{tenant_id}/orders")
def list_orders(tenant_id: int = Depends(guardrail.check), limit: int = Query(50, le=500)):
    """Light, OLTP-style query: what small tenants do all day."""
    with db.tenant_conn(tenant_id) as conn:
        return conn.execute(
            "SELECT id, order_date, status, total FROM orders "
            "WHERE tenant_id = %s ORDER BY order_date DESC, id DESC LIMIT %s",
            (tenant_id, limit),
        ).fetchall()


@app.post("/t/{tenant_id}/orders", status_code=201)
def create_order(order: OrderIn, tenant_id: int = Depends(guardrail.check)):
    total = sum(l.qty * l.unit_price for l in order.lines)
    try:
        with db.tenant_conn(tenant_id) as conn:
            row = conn.execute(
                "INSERT INTO orders (tenant_id, order_date, status, total) "
                "VALUES (%s, CURRENT_DATE, 'new', %s) RETURNING id",
                (tenant_id, total),
            ).fetchone()
            conn.cursor().executemany(
                "INSERT INTO order_lines (tenant_id, order_id, product, qty, unit_price) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(tenant_id, row["id"], l.product, l.qty, l.unit_price) for l in order.lines],
            )
    except psycopg.errors.ForeignKeyViolation:
        raise HTTPException(status_code=404, detail="unknown tenant")
    return {"id": row["id"], "total": total}


# Improvement 2: short cache for the month-end report.
# The whale's finance team asks for the same report many times at month-end.
# One identical report (tenant + year) is computed once per REPORT_CACHE_SECONDS;
# requests that arrive while it is being computed wait for that result (single-flight).
# A cached report can be up to REPORT_CACHE_SECONDS old; the cache is cleared when
# the whale is removed or added on the Ops page.
REPORT_CACHE_SECONDS = float(os.getenv("REPORT_CACHE_SECONDS", "0"))
_report_cache: dict[tuple, tuple[float, bytes]] = {}  # ready-to-send JSON
_report_locks: dict[tuple, threading.Lock] = {}
_report_locks_guard = threading.Lock()

MONTHLY_SQL = """
    SELECT date_trunc('month', o.order_date)::date AS month,
           l.product,
           sum(l.qty * l.unit_price) AS revenue,
           sum(l.qty)                AS units
    FROM orders o
    JOIN order_lines l ON l.order_id = o.id AND l.tenant_id = o.tenant_id
    WHERE o.tenant_id = %s AND o.order_date >= %s AND o.order_date < %s
    GROUP BY 1, 2
    ORDER BY 1, revenue DESC
    LIMIT 200
"""


def clear_report_cache(tenant_id: int | None = None) -> None:
    for key in list(_report_cache):
        if tenant_id is None or key[0] == tenant_id:
            _report_cache.pop(key, None)


def _run_monthly(tenant_id: int, year: int) -> list:
    with db.tenant_conn(tenant_id) as conn:
        return conn.execute(MONTHLY_SQL, (tenant_id, date(year, 1, 1), date(year + 1, 1, 1))).fetchall()


@app.get("/t/{tenant_id}/reports/monthly")
def monthly_report(tenant_id: int = Depends(guardrail.check), year: int = 2026):
    """Heavy, OLAP-style query: the month-end report that slowed everyone down."""
    if REPORT_CACHE_SECONDS <= 0:
        return _run_monthly(tenant_id, year)
    key = (tenant_id, year)
    hit = _report_cache.get(key)
    if hit and time.monotonic() - hit[0] < REPORT_CACHE_SECONDS:
        return Response(hit[1], media_type="application/json")
    with _report_locks_guard:
        lock = _report_locks.setdefault(key, threading.Lock())
    with lock:  # only one request computes; the others wait and reuse its result
        hit = _report_cache.get(key)
        if not (hit and time.monotonic() - hit[0] < REPORT_CACHE_SECONDS):
            body = json.dumps(jsonable_encoder(_run_monthly(tenant_id, year))).encode()
            hit = _report_cache[key] = (time.monotonic(), body)
        return Response(hit[1], media_type="application/json")


@app.get("/admin/metrics")
def metrics():
    """Whale vs. small tenants latency + connection pool pressure + spike settings."""
    return {**guardrail.snapshot(), "pools": db.pool_stats(),
            "settings": {**guardrail.settings(), "report_cache_seconds": REPORT_CACHE_SECONDS}}


@app.post("/admin/metrics/reset")
def metrics_reset():
    guardrail.reset()
    return {"reset": True}


@app.get("/admin/whale")
def whale_status():
    return whale.status()


@app.post("/admin/whale/remove")
def whale_remove():
    whale.remove()
    clear_report_cache(guardrail.WHALE_ID)
    return whale.status()


@app.post("/admin/whale/add")
def whale_add():
    whale.add()
    clear_report_cache(guardrail.WHALE_ID)
    return whale.status()


@app.get("/admin/simulation")
def simulation_status():
    return {"running": simulation.running()}


@app.post("/admin/simulation/start")
def simulation_start():
    simulation.start()
    return {"running": simulation.running()}


@app.post("/admin/simulation/stop")
def simulation_stop():
    simulation.stop()
    return {"running": False}
