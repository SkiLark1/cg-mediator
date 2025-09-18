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
AGENT_STATUS_PATH = os.getenv("AGENT_STATUS_PATH", "/api/public/agents/live")
AGENT_STATUS_PATH = os.getenv("AGENT_STATUS_PATH", "/api/public/agents/live")

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

async def tld_agents_live(client: httpx.AsyncClient) -> Any:
    """
    Fetch per-agent live data. If your tenant uses a different route,
    set AGENT_STATUS_PATH in the service env.
    """
    url = f"{TLD_BASE}{AGENT_STATUS_PATH}"
    r = await client.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # normalize to a list of rows
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("agents","data","rows"):
            if isinstance(data.get(k), list):
                return data[k]
    # last resort: wrap dict
    return [data]

def parse_hhmmss_to_seconds(s: str) -> int | None:
    if not isinstance(s, str):
        return None
    s = s.strip()
    # H:MM:SS or MM:SS
    m = re.match(r"^(?:(\d+):)?([0-5]?\d):([0-5]?\d)$", s)
    if m:
        h = int(m.group(1) or 0); mm = int(m.group(2)); ss = int(m.group(3))
        return h*3600 + mm*60 + ss
    if s.isdigit():
        return int(s)
    return None

def pick(obj: dict, keys: list[str]) -> Any:
    for k in keys:
        if k in obj:
            return obj[k]
    # try case-insensitive
    low = {str(k).lower(): v for k,v in obj.items()}
    for k in keys:
        if k.lower() in low:
            return low[k.lower()]
    return None

def match_ingroup_state(row: dict, ing_base: str, st: str | None) -> bool:
    """
    Accept if row's ingroup/campaign matches the requested ing_base (+ optional state).
    We check several likely column names seen in TLD UIs/APIs.
    """
    ing_fields = [
        "ingroup", "call ingroup name", "call ingroup", "call campaign / ingroup",
        "campaign / ingroup", "vendor", "campaign", "campaign name"
    ]
    val = pick(row, ing_fields)
    if not val:
        return False
    val_up = str(val).strip().upper()
    ing_up = ing_base.strip().upper()
    if st:
        st_up = st.strip().upper()
        return val_up == f"{ing_up}{st_up}" or val_up.endswith(f" {st_up}")
    # base only
    return val_up == ing_up or val_up.startswith(ing_up)

def row_status(row: dict) -> str:
    s = pick(row, ["Live Status","status","Status","agent status"]) or ""
    return str(s).strip().lower()

def row_status_duration_seconds(row: dict) -> int | None:
    dur = pick(row, ["Status Duration","status duration","duration","status_duration","live status duration"])
    return parse_hhmmss_to_seconds(str(dur)) if dur is not None else None

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
    # state selection
    no_state: Optional[int] = Query(0, description="If 1, disable state suffix for counts (kept for completeness)"),
    state: Optional[str] = Query(None, description="Force a single state, e.g., FL"),
    states: Optional[str] = Query(None, description="CSV list of states, e.g., MI,TX,OK"),
    # debug
    raw: Optional[int] = Query(0, description="If 1, include raw upstream JSON for debugging")
):
    """
    Decision:
      accept = (ready_count >= ready_min)
             or (exists agent in {CLOSER, READY} for ing+state whose StatusDuration >= threshold)
    """
    ING_THIS = (ing or ING_BASE).strip()
    READY_MIN_THIS = READY_MIN if ready_min is None else int(ready_min)
    IDLE_THRESHOLD_THIS = IDLE_THRESHOLD if threshold is None else int(threshold)

    # build state list
    state_list: list[Optional[str]] = []
    if states:
        state_list = [s.strip().upper() for s in states.split(",") if s.strip()]
    elif state:
        state_list = [state.strip().upper()]
    elif no_state:
        state_list = [None]  # base ingroup only
    else:
        state_list = [None]  # default (base) — you can pass state(s) explicitly in CallGrid

    now = time.time()

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:

            # 1) get agents once (we filter client-side per state)
            agents_rows = await tld_agents_live(client)

            # 2) function to evaluate one state
            async def eval_state(st: Optional[str]) -> dict:
                # counts endpoint for "ready" (fast path for >= ready_min)
                counts = await tld_ready(client, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "sta": "false" if st else "true"  # if state is forced, don't let TLD append another suffix
                })
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                ready_ge_min = ready_count >= READY_MIN_THIS

                # filter agent rows: ingroup (+state), status in {CLOSER, READY}
                max_duration = 0
                matched = []
                for row in agents_rows:
                    try:
                        if not match_ingroup_state(row, ING_THIS, st):
                            continue
                        st_lower = row_status(row)
                        if st_lower not in READYLIKE_STATUSES:
                            continue
                        dur = row_status_duration_seconds(row)
                        if dur is None:
                            continue
                        max_duration = max(max_duration, int(dur))
                        matched.append({
                            "status": st_lower,
                            "duration": int(dur),
                            "ingroupField": pick(row, ["Call Ingroup Name","ingroup","Call Campaign / Ingroup","Campaign / Ingroup","campaign","vendor"]),
                            "agent": pick(row, ["Agent Full Name","User","user","agent"]),
                        })
                    except Exception:
                        continue

                # decision for this state
                waiting_too_long = (max_duration >= IDLE_THRESHOLD_THIS) and (max_duration > 0)
                candidate = bool(ready_ge_min or (waiting_too_long and len(matched) > 0))

                dbg = {
                    "ing": ING_THIS,
                    "state": (st or "BASE"),
                    "route_key": f"{ING_THIS}{st or ''}|{area_code_from_phone(phone)}",
                    "ready": ready_count,
                    "ready_min": READY_MIN_THIS,
                    "threshold": IDLE_THRESHOLD_THIS,
                    "max_agent_status_duration": max_duration,
                    "matched_agents": matched[:10],  # cap for readability
                }
                if raw:
                    dbg["counts_raw"] = counts

                return {
                    "candidate": candidate,
                    "ready": ready_count,
                    "maxStatusDuration": max_duration,
                    "waitingTooLong": waiting_too_long,
                    "debug": dbg
                }

            # 3) evaluate all requested states
            per_state: dict[str, dict] = {}
            overall_ready = 0
            overall_waiting = False
            overall_candidate = False
            overall_maxdur = 0

            for st in state_list:
                label = st or "BASE"
                res = await eval_state(st)
                per_state[label] = res
                overall_ready = max(overall_ready, res["ready"])
                overall_waiting = overall_waiting or res["waitingTooLong"]
                overall_candidate = overall_candidate or res["candidate"]
                overall_maxdur = max(overall_maxdur, res["maxStatusDuration"])

            computed_should_accept = overall_candidate
            should_accept = False if dry else computed_should_accept

            resp = {
                "shouldAccept": should_accept,
                "ready": overall_ready,
                "waitingTooLong": overall_waiting,
                "idleObservedSeconds": overall_maxdur,  # now reflects max agent Status Duration
                "debug": {
                    "ing": ING_THIS,
                    "states_evaluated": list(per_state.keys()),
                    "per_state": per_state,
                    "computed_should_accept": computed_should_accept,
                    "dry_mode": bool(dry),
                }
            }
            if raw:
                resp["debug"]["agents_raw_sample"] = agents_rows[:10] if isinstance(agents_rows, list) else agents_rows
            return resp

    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })

