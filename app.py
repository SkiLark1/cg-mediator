import os, time, re
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Query
import httpx

# ---- Configuration (env with sane defaults)
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")  # your current ingroup base
READY_MIN      = int(os.getenv("READY_MIN", "2"))   # "more than 1 agent"
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "30"))  # seconds
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "2.0"))

# Track when (ava>=1 & queue==0) started, per key
idle_since: Dict[str, float] = {}

app = FastAPI(title="CallGrid Acceptance Mediator")

def area_code_from_phone(p: str) -> str:
    m = re.search(r"(\d{10})$", re.sub(r"\D", "", p or ""))
    return (m.group(1)[0:3] if m else "UNK")

def route_key_from_json(base: str, counts: Dict[str, Any], phone: str) -> str:
    # Prefer explicit state/vendor if TLD returns them; else fall back to ANI area code
    st = str(counts.get("state") or counts.get("st") or area_code_from_phone(phone))
    ing = str(counts.get("ingroup") or counts.get("vendor") or base)
    return f"{ing}|{st}"

async def tld_ready(client: httpx.AsyncClient, phone: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    base_params = {
        "ava": 1,
        "ing": ING_BASE,
        "sta": "true",
        "cnt": "true",
        "act": "true",
        "pol": "true",
        "rsn": "true",
    }
    params = {**base_params, **extra}
    url = f"{TLD_BASE}/api/public/dialer/ready/{phone}"
    r = await client.get(url, params=params)
    r.raise_for_status()
    return r.json()

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/accept")
async def accept(phone: str = Query(..., min_length=7, max_length=25)):
    now = time.time()

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            # 1) Normal counts
            counts = await tld_ready(client, phone, extra={})
            ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
            ready_ge_min = ready_count >= READY_MIN

            # 2) Idle-now signal: queue==0 in ingroup/state AND ava>=1
            idle_resp = await tld_ready(client, phone, extra={"que": 0, "qui": "ing", "ava": 1})
            # Robustly detect the "constraints satisfied" bit:
            idle_now = False
            # Prefer explicit fields if present
            if "queue" in idle_resp:
                try:
                    idle_now = int(idle_resp["queue"]) == 0 and int(idle_resp.get("ready", 0)) >= 1
                except Exception:
                    idle_now = False
            # Fallback: many setups return { "val": 1 } when constraints matched
            if not idle_now:
                idle_now = str(idle_resp.get("val", "0")).lower() in ("1", "true")

            # 3) Build a stable key to time persistence
            rk = route_key_from_json(ING_BASE, counts, phone)

            if idle_now:
                idle_since.setdefault(rk, now)
            else:
                idle_since.pop(rk, None)

            idle_age = int(now - idle_since[rk]) if rk in idle_since else 0
            waiting_too_long = idle_age >= IDLE_THRESHOLD

            should_accept = bool(ready_ge_min or waiting_too_long)

            return {
                "shouldAccept": should_accept,
                "ready": ready_count,
                "waitingTooLong": waiting_too_long,
                "idleObservedSeconds": idle_age,
                # Optional debug fields to help you tune:
                "debug": {
                    "ready_ge_min": ready_ge_min,
                    "idle_now": idle_now,
                    "route_key": rk
                }
            }

    except httpx.HTTPError as e:
        # Explicitly reject on dependency failure
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })
