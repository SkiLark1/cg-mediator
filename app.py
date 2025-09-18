import os, time, re
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
import httpx

# ---- Configuration (env with sane defaults)
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")          # default ingroup base
READY_MIN      = int(os.getenv("READY_MIN", "2"))           # "more than 1 agent"
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "30"))     # seconds
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "2.0"))

# Track when (ava>=1 & queue==0) started, per key
idle_since: Dict[str, float] = {}

app = FastAPI(title="CallGrid Acceptance Mediator")

def area_code_from_phone(p: str) -> str:
    m = re.search(r"(\d{10})$", re.sub(r"\D", "", p or ""))
    return (m.group(1)[0:3] if m else "UNK")

def route_key_from_json(base: str, counts: Dict[str, Any], phone: str) -> str:
    """Prefer explicit state/vendor if TLD returns them; else fall back to ANI area code."""
    st = str(counts.get("state") or counts.get("st") or area_code_from_phone(phone))
    ing = str(counts.get("ingroup") or counts.get("vendor") or base)
    return f"{ing}|{st}"

async def tld_ready(client: httpx.AsyncClient, phone: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call TLD Dialer Ready with your current flags; allow per-call overrides via `extra`.
    Note: keys in `extra` override the defaults below.
    """
    base_params = {
        "ava": 1,
        "ing": ING_BASE,   # can be overridden by extra["ing"]
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
async def accept(
    phone: str = Query(..., min_length=7, max_length=25),
    ing: Optional[str] = Query(None, description="TLD ingroup base, e.g., SREZMEDI_ or SRMEDTI_"),
    ready_min: Optional[int] = Query(None, description="Override READY_MIN"),
    threshold: Optional[int] = Query(None, description="Override IDLE_THRESHOLD (seconds)"),
    dry: Optional[int] = Query(0, description="If 1, never accept (for safe testing)"),
    no_state: Optional[int] = Query(0, description="If 1, disable state suffix (sta=false)"),
    state: Optional[str] = Query(None, description="Force a specific state suffix, e.g., FL"),
    scope: Optional[str] = Query("ing", description="Idle scope: ing | sta | all"),
    raw: Optional[int] = Query(0, description="If 1, include raw upstream JSON for debugging")
):
    now = time.time()

    # Per-call overrides (fall back to env defaults)
    ING_THIS = (ing or ING_BASE).strip()
    READY_MIN_THIS = READY_MIN if ready_min is None else int(ready_min)
    IDLE_THRESHOLD_THIS = IDLE_THRESHOLD if threshold is None else int(threshold)

    # Decide state behavior
    force_state = (state or "").strip().upper() or None
    use_sta_flag = False if no_state else True  # default True unless no_state=1

    # Normalize scope
    scope = (scope or "ing").lower()
    if scope not in ("ing", "sta", "all"):
        scope = "ing"

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            # Helper to call with a given state strategy
            async def get_counts_and_idle(sta_flag: bool, forced_state: Optional[str]):
                # Build params for counts
                if forced_state:
                    counts_params = {"ing": f"{ING_THIS}{forced_state}", "sta": "false"}
                else:
                    counts_params = {"ing": ING_THIS, "sta": "true" if sta_flag else "false"}

                counts_resp = await tld_ready(client, phone, extra=counts_params)

                # Build params for idle probe
                idle_params = counts_params.copy()
                idle_params.update({"ava": 1, "que": 0})
                if scope in ("ing", "sta"):
                    idle_params["qui"] = scope  # qui=ing or qui=sta
                # if scope=all -> don't send qui, just que=0

                idle_resp = await tld_ready(client, phone, extra=idle_params)

                # Label mode
                state_mode = forced_state or ("AUTO" if sta_flag else "NONE")
                return counts_resp, idle_resp, state_mode

            # 1) First try with selected strategy (forced-state > no_state > auto)
            counts, idle_resp, state_mode = await get_counts_and_idle(use_sta_flag, force_state)
            ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)

            # 2) If auto (sta=true) yielded 0 ready and we didn't force state, fallback once with sta=false
            fallback_used = False
            if ready_count == 0 and force_state is None and use_sta_flag:
                counts, idle_resp, state_mode = await get_counts_and_idle(False, None)
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                fallback_used = True

            ready_ge_min = ready_count >= READY_MIN_THIS

            # Detect idle_now robustly
            idle_now = False
            if "queue" in idle_resp:
                try:
                    idle_now = int(idle_resp["queue"]) == 0 and int(idle_resp.get("ready", 0)) >= 1
                except Exception:
                    idle_now = False
            if not idle_now:
                idle_now = str(idle_resp.get("val", "0")).lower() in ("1", "true")

            # Build route key using what we actually queried
            effective_ing_for_key = f"{ING_THIS}{force_state}" if force_state else ING_THIS
            rk = route_key_from_json(effective_ing_for_key, counts, phone)

            if idle_now:
                idle_since.setdefault(rk, now)
            else:
                idle_since.pop(rk, None)

            idle_age = int(now - idle_since[rk]) if rk in idle_since else 0
            waiting_too_long = idle_age >= IDLE_THRESHOLD_THIS

            computed_should_accept = bool(ready_ge_min or waiting_too_long)
            should_accept = False if dry else computed_should_accept  # dry-run forces false

            resp = {
                "shouldAccept": should_accept,
                "ready": ready_count,
                "waitingTooLong": waiting_too_long,
                "idleObservedSeconds": idle_age,
                "debug": {
                    "ing": ING_THIS,
                    "ready_min": READY_MIN_THIS,
                    "threshold": IDLE_THRESHOLD_THIS,
                    "ready_ge_min": ready_ge_min,
                    "idle_now": idle_now,
                    "route_key": rk,
                    "state_mode": state_mode,         # "AUTO", "NONE", or forced state like "FL"
                    "fallback_used": fallback_used,   # True if we retried with sta=false
                    "scope": scope,
                    "computed_should_accept": computed_should_accept,
                    "dry_mode": bool(dry),
                }
            }

            if raw:
                # include raw upstream for diagnosis (trim very large payloads if needed)
                resp["debug"]["counts_raw"] = counts
                resp["debug"]["idle_raw"] = idle_resp

            return resp

    except httpx.HTTPError as e:
        # Explicitly reject on dependency failure
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })
