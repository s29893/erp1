"""Per-tenant metrics and the guardrail hook.

SPIKE HOOK #2 - protect small tenants.
`check()` runs before every tenant endpoint. Light requests always pass.
Heavy reports go through a per-tenant queue (see "Improvement 1" below);
with REPORT_CONCURRENCY=0 and REPORT_RATE=0 everything passes, as before the spike.
"""
import asyncio
import os
import re
import threading
from collections import defaultdict, deque

from fastapi import HTTPException, Request

WHALE_ID = 1
_PATH = re.compile(r"^/t/(\d+)/")

_lat: dict[int, deque] = defaultdict(lambda: deque(maxlen=1000))
_lock = threading.Lock()


def tenant_from_path(path: str):
    m = _PATH.match(path)
    return int(m.group(1)) if m else None


def record(tenant_id: int, ms: float, status: int) -> None:
    with _lock:
        _lat[tenant_id].append((ms, status))


# Improvement 1: per-tenant queue for heavy reports.
# At most REPORT_CONCURRENCY heavy reports of one tenant run at once (0 = no limit),
# and at most REPORT_RATE reports per second start for one tenant (0 = no limit).
# Extra reports WAIT in line (up to REPORT_QUEUE_TIMEOUT s) instead of being rejected.
# The wait is async, so a waiting report does not hold one of the server's worker
# threads: small tenants always find a free thread. Light requests are never limited.
REPORT_CONCURRENCY = int(os.getenv("REPORT_CONCURRENCY", "0"))
REPORT_RATE = float(os.getenv("REPORT_RATE", "0"))
REPORT_QUEUE_TIMEOUT = float(os.getenv("REPORT_QUEUE_TIMEOUT", "30"))
_next_start: dict[int, float] = {}
_sems: dict[int, asyncio.Semaphore] = {}
_rejected: dict[int, int] = defaultdict(int)


async def check(tenant_id: int, request: Request):
    """FastAPI dependency: queue heavy reports per tenant; 429 only if the wait is too long."""
    if "/reports/" not in request.url.path or (REPORT_CONCURRENCY <= 0 and REPORT_RATE <= 0):
        yield tenant_id
        return
    if REPORT_RATE > 0:  # pace: each report gets the next free start time for its tenant
        loop = asyncio.get_running_loop()
        now = loop.time()
        start = max(now, _next_start.get(tenant_id, now))
        if start - now > REPORT_QUEUE_TIMEOUT:
            _rejected[tenant_id] += 1
            raise HTTPException(status_code=429, detail="report queue full, retry later")
        _next_start[tenant_id] = start + 1 / REPORT_RATE
        await asyncio.sleep(start - now)
    if REPORT_CONCURRENCY <= 0:
        yield tenant_id
        return
    sem = _sems.setdefault(tenant_id, asyncio.Semaphore(REPORT_CONCURRENCY))
    try:
        await asyncio.wait_for(sem.acquire(), timeout=REPORT_QUEUE_TIMEOUT)
    except asyncio.TimeoutError:
        _rejected[tenant_id] += 1
        raise HTTPException(status_code=429, detail="report queue full, retry later")
    try:
        yield tenant_id
    finally:
        sem.release()


def settings() -> dict:
    return {"report_concurrency": REPORT_CONCURRENCY, "report_rate": REPORT_RATE,
            "report_queue_timeout": REPORT_QUEUE_TIMEOUT}


def _pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    return round(s[min(len(s) - 1, int(p / 100 * len(s)))], 1)


def _summ(samples: list[tuple[float, int]]) -> dict:
    ms = [m for m, _ in samples]
    return {
        "requests": len(ms),
        "errors": sum(1 for _, st in samples if st >= 400),
        "p50_ms": _pct(ms, 50),
        "p95_ms": _pct(ms, 95),
        "p99_ms": _pct(ms, 99),
    }


def snapshot() -> dict:
    """Latency summary: whale vs. all small tenants (last ~1000 requests per tenant)."""
    with _lock:
        whale = list(_lat.get(WHALE_ID, []))
        small = [s for tid, d in _lat.items() if tid != WHALE_ID for s in d]
        per_tenant = {tid: list(d) for tid, d in _lat.items() if tid != WHALE_ID}
    worst = sorted(
        ({"tenant_id": tid, **_summ(s)} for tid, s in per_tenant.items() if s),
        key=lambda x: x["p95_ms"] or 0,
        reverse=True,
    )[:5]
    return {"whale": _summ(whale), "small_tenants": _summ(small), "worst_small_tenants": worst,
            "report_rejections": dict(_rejected)}


def reset() -> None:
    with _lock:
        _lat.clear()
        _rejected.clear()
