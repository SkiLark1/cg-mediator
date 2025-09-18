import os, time, re
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
import httpx

# ---- Configuration (env with sane defaults)
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")          # default ingroup base
READY_MIN      = int(os.getenv("READY_MIN", "2"))           # "more than 1 agent"
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "60"))     # seconds
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
    # single-state controls (still supported)
    no_state: Optional[int] = Query(0, description="If 1, disable state suffix (sta=false)"),
    state: Optional[str] = Query(None, description="Force a specific state suffix, e.g., FL"),
    # NEW: multi-state
    states: Optional[str] = Query(None, description="Comma-separated state list, e.g., MI,TX,OK"),
    scope: Optional[str] = Query("ing", description="Idle scope: ing | sta | all"),
    raw: Optional[int] = Query(0, description="If 1, include raw upstream JSON for debugging")
):
    now = time.time()

    ING_THIS = (ing or ING_BASE).strip()
    READY_MIN_THIS = READY_MIN if ready_min is None else int(ready_min)
    IDLE_THRESHOLD_THIS = IDLE_THRESHOLD if threshold is None else int(threshold)

    # normalize scope
    scope = (scope or "ing").lower()
    if scope not in ("ing", "sta", "all"):
        scope = "ing"

    # decide which state modes to evaluate
    state_list = []
    if states:
        state_list = [s.strip().upper() for s in states.split(",") if s.strip()]
    elif state:
        state_list = [state.strip().upper()]
    elif no_state:
        state_list = [None]  # explicit base ingroup only
    else:
        # default “AUTO then fallback NONE” behavior as a single path
        state_list = [None]  # we’ll still do AUTO vs NONE inside

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:

            async def check_one_state(forced_state: Optional[str]):
                """
                Returns dict with: ready, idle_now, idle_age, waiting_too_long, should_accept_candidate, debug
                Uses AUTO (sta=true) with fallback to sta=false when forced_state is None.
                """
                # --- inner helper to fetch counts/idle under a given sta flag and/or forced state
                async def fetch(sta_flag: bool, forced: Optional[str]):
                    if forced:
                        counts_params = {"ing": f"{ING_THIS}{forced}", "sta": "false"}
                        idle_params   = {"ing": f"{ING_THIS}{forced}", "sta": "false", "ava": 1, "que": 0}
                    else:
                        counts_params = {"ing": ING_THIS, "sta": "true" if sta_flag else "false"}
                        idle_params   = {"ing": ING_THIS, "sta": "true" if sta_flag else "false", "ava": 1, "que": 0}
                    if scope in ("ing", "sta"):
                        idle_params["qui"] = scope
                    counts_resp = await tld_ready(client, phone, extra=counts_params)
                    idle_resp   = await tld_ready(client, phone, extra=idle_params)
                    return counts_resp, idle_resp

                # 1) initial attempt
                sta_auto = (forced_state is None)
                counts, idle_resp = await fetch(sta_flag=sta_auto, forced=forced_state)
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                state_mode = forced_state or ("AUTO" if sta_auto else "NONE")
                fallback_used = False

                # 2) fallback if AUTO returned 0
                if sta_auto and ready_count == 0:
                    counts, idle_resp = await fetch(sta_flag=False, forced=None)
                    ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                    state_mode = "NONE"
                    fallback_used = True

                ready_ge_min = ready_count >= READY_MIN_THIS

                # Detect “idle candidate”
                idle_now = False
                if isinstance(idle_resp, dict):
                    if "queue" in idle_resp:
                        try:
                            idle_now = int(idle_resp["queue"]) == 0 and int(idle_resp.get("ready", 0)) >= 1
                        except Exception:
                            idle_now = False
                    if not idle_now:
                        idle_now = str(idle_resp.get("val", "0")).lower() in ("1", "true")
                if not idle_now:
                    idle_now = (ready_count >= 1 and not ready_ge_min)

                # route-key for timing (include forced state if present)
                effective_ing = f"{ING_THIS}{forced_state}" if forced_state else ING_THIS
                rk = route_key_from_json(effective_ing, counts, phone)

                # update timer
                if idle_now:
                    idle_since.setdefault(rk, now)
                else:
                    idle_since.pop(rk, None)
                idle_age = int(now - idle_since[rk]) if rk in idle_since else 0
                waiting_too_long = idle_age >= IDLE_THRESHOLD_THIS

                # candidate accept for this state
                candidate_accept = bool(ready_ge_min or waiting_too_long)

                dbg = {
                    "ing": ING_THIS,
                    "state_mode": state_mode if forced_state else state_mode,
                    "route_key": rk,
                    "ready_min": READY_MIN_THIS,
                    "threshold": IDLE_THRESHOLD_THIS,
                    "ready_ge_min": ready_ge_min,
                    "idle_now": idle_now,
                    "fallback_used": fallback_used,
                    "scope": scope,
                }
                if raw:
                    dbg["counts_raw"] = counts
                    dbg["idle_raw"] = idle_resp

                return {
                    "ready": ready_count,
                    "idleObservedSeconds": idle_age,
                    "waitingTooLong": waiting_too_long,
                    "candidate": candidate_accept,
                    "debug": dbg
                }

            # Evaluate one or many states
            per_state = {}
            overall_ready = 0
            overall_waiting = False
            overall_idle_age = 0
            overall_candidate = False

            for st in state_list:
                res = await check_one_state(st)
                label = (st or "BASE")
                per_state[label] = res
                overall_ready = max(overall_ready, res["ready"])
                overall_waiting = overall_waiting or res["waitingTooLong"]
                overall_idle_age = max(overall_idle_age, res["idleObservedSeconds"])
                overall_candidate = overall_candidate or res["candidate"]

            # If we only evaluated BASE (no states), the above loop still handled AUTO->NONE fallback inside.

            computed_should_accept = overall_candidate
            should_accept = False if dry else computed_should_accept

            return {
                "shouldAccept": should_accept,
                "ready": overall_ready,
                "waitingTooLong": overall_waiting,
                "idleObservedSeconds": overall_idle_age,
                "debug": {
                    "ing": ING_THIS,
                    "states_evaluated": list(per_state.keys()),
                    "per_state": per_state,
                    "computed_should_accept": computed_should_accept,
                    "dry_mode": bool(dry),
                }
            }

    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })
