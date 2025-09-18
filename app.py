import os, time, re
from typing import Dict, Any, Optional, List, Tuple, Set
from fastapi import FastAPI, HTTPException, Query
import httpx

# ---- Configuration (env with sane defaults)
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")
READY_MIN      = int(os.getenv("READY_MIN", "2"))
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "60"))     # seconds
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "2.0"))

# Egress auth (for /api/egress/tldialer/*)
TLD_API_ID  = os.getenv("TLD_API_ID")
TLD_API_KEY = os.getenv("TLD_API_KEY")

# Public “ready” counts endpoint (unchanged)
READYLIKE_STATUSES = {"closer", "ready"}

# Egress endpoints we’ll use together
PATH_VICI_LIVE_AGENTS         = "/api/egress/tldialer/vicidial_live_agents"
PATH_VICI_LIVE_INBOUND_AGENTS = "/api/egress/tldialer/vicidial_live_inbound_agents"

# Counts/idle fallback state
idle_since: Dict[str, float] = {}

app = FastAPI(title="CallGrid Acceptance Mediator")

# ---------- helpers

def area_code_from_phone(p: str) -> str:
    m = re.search(r"(\d{10})$", re.sub(r"\D", "", p or ""))
    return (m.group(1)[0:3] if m else "UNK")

def route_key(ing_base: str, st: Optional[str], phone: str) -> str:
    return f"{ing_base}{(st or '')}|{area_code_from_phone(phone)}"

def parse_hhmmss_to_seconds(s: Any) -> Optional[int]:
    if s is None:
        return None
    s = str(s).strip()
    m = re.match(r"^(?:(\d+):)?([0-5]?\d):([0-5]?\d)$", s)  # H:MM:SS or MM:SS
    if m:
        h = int(m.group(1) or 0); mm = int(m.group(2)); ss = int(m.group(3))
        return h*3600 + mm*60 + ss
    if s.isdigit():
        return int(s)
    return None

def pick(obj: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in obj:
            return obj[k]
    low = {str(k).lower(): v for k, v in obj.items()}
    for k in keys:
        lk = k.lower()
        if lk in low:
            return low[lk]
    return None

def auth_headers(path: str) -> Dict[str, str]:
    if path.startswith("/api/egress/tldialer/") and TLD_API_ID and TLD_API_KEY:
        # TLD normalizes these to lowercase server-side
        return {"tld-api-id": TLD_API_ID, "tld-api-key": TLD_API_KEY}
    return {}

# ---------- TLD calls

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
    r = await client.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

async def fetch_live_agents(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    url = f"{TLD_BASE}{PATH_VICI_LIVE_AGENTS}"
    r = await client.get(url, headers=auth_headers(PATH_VICI_LIVE_AGENTS), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # payload shape: {"response":{"results":[...]}}
    rows = []
    if isinstance(data, dict):
        rows = data.get("response", {}).get("results", [])
        if isinstance(rows, list):
            rows = [x for x in rows if isinstance(x, dict)]
        else:
            rows = []
    return rows

async def fetch_inbound_membership(client: httpx.AsyncClient) -> Dict[str, Set[str]]:
    """
    Return a map: user_id (string) -> set of group_ids this user can take.
    Endpoint doesn’t include live status/durations; we only need membership.
    """
    url = f"{TLD_BASE}{PATH_VICI_LIVE_INBOUND_AGENTS}"
    r = await client.get(url, headers=auth_headers(PATH_VICI_LIVE_INBOUND_AGENTS), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    rows = []
    if isinstance(data, dict):
        rows = data.get("response", {}).get("results", [])
        if not isinstance(rows, list):
            rows = []

    m: Dict[str, Set[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        user = str(pick(row, ["user","agent","User","Agent","agent_id"]) or "").strip()
        grp  = str(pick(row, ["group_id","group","ingroup"]) or "").strip().upper()
        if not user or not grp:
            continue
        m.setdefault(user, set()).add(grp)
    return m

# ---------- matching / extraction

def extract_user_id(agent_row: Dict[str, Any]) -> str:
    # live_agents carries "user" as the login / id
    return str(pick(agent_row, ["user","User","agent","agent_id"]) or "").strip()

def row_status(agent_row: Dict[str, Any]) -> str:
    # vicidial_live_agents field
    s = pick(agent_row, ["live_status","status","Live Status","Status"]) or ""
    return str(s).strip().lower()

def row_status_duration_seconds(agent_row: Dict[str, Any]) -> Optional[int]:
    # vicidial_live_agents field
    dur = pick(agent_row, ["last_state_duration","status_duration","duration"])
    return parse_hhmmss_to_seconds(dur)

def user_matches_state(user_groups: Set[str], ing_base: str, state: Optional[str]) -> bool:
    if not user_groups:
        return False
    ing_up = ing_base.strip().upper()
    if state:
        target = f"{ing_up}{state.strip().upper()}"
        return target in user_groups
    # base-only: any group that startswith the base
    return any(g.startswith(ing_up) for g in user_groups)

# ---------- Routes

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
    state: Optional[str] = Query(None, description="Single state (e.g., FL)"),
    states: Optional[str] = Query(None, description="CSV states (e.g., MI,TX,OK)"),
    raw: Optional[int] = Query(0, description="If 1, include raw upstream JSON for debugging")
):
    """
    Accept if:
      - ready_count >= ready_min        (from /dialer/ready)
      - OR: any agent (status in {CLOSER, READY}) mapped to ing+state has StatusDuration >= threshold
      - OR (fallback): ava>=1 & queue==0 for ing+state for >= threshold seconds
    """
    ING_THIS = (ing or ING_BASE).strip()
    READY_MIN_THIS = READY_MIN if ready_min is None else int(ready_min)
    IDLE_THRESHOLD_THIS = IDLE_THRESHOLD if threshold is None else int(threshold)

    # states list
    if states:
        state_list: List[Optional[str]] = [s.strip().upper() for s in states.split(",") if s.strip()]
    elif state:
        state_list = [state.strip().upper()]
    else:
        state_list = [None]  # base only

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            # 1) Always get the ready counts (cheap)
            # 2) Try the egress pair; if either fails, we’ll gracefully fall back to counts/idle timer.
            try:
                live_agents = await fetch_live_agents(client)
                membership  = await fetch_inbound_membership(client)
                have_egress = True
            except httpx.HTTPError:
                live_agents = []
                membership  = {}
                have_egress = False

            async def eval_state(st: Optional[str]) -> Dict[str, Any]:
                # ready counts
                counts = await tld_ready(client, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "sta": "false" if st else "true"
                })
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                ready_ge_min = ready_count >= READY_MIN_THIS

                if have_egress:
                    # AGENTS MODE WITH MEMBERSHIP JOIN
                    max_duration = 0
                    matched_dbg: List[Dict[str, Any]] = []

                    for row in live_agents:
                        try:
                            status_l = row_status(row)
                            if status_l not in READYLIKE_STATUSES:
                                continue

                            user_id = extract_user_id(row)
                            if not user_id:
                                continue
                            user_groups = membership.get(user_id, set())
                            if not user_matches_state(user_groups, ING_THIS, st):
                                continue

                            dur = row_status_duration_seconds(row)
                            if dur is None:
                                continue
                            max_duration = max(max_duration, int(dur))

                            if raw:
                                matched_dbg.append({
                                    "user": user_id,
                                    "status": status_l,
                                    "duration": int(dur),
                                    "groups": sorted(list(user_groups))[:8],
                                })
                        except Exception:
                            continue

                    waiting_too_long = (max_duration >= IDLE_THRESHOLD_THIS) and (max_duration > 0)
                    candidate = bool(ready_ge_min or waiting_too_long)

                    dbg = {
                        "mode": "agents_join",
                        "ing": ING_THIS,
                        "state": (st or "BASE"),
                        "route_key": route_key(ING_THIS, st, phone),
                        "ready_min": READY_MIN_THIS,
                        "threshold": IDLE_THRESHOLD_THIS,
                        "ready_ge_min": ready_ge_min,
                    }
                    if raw:
                        dbg["counts_raw"] = counts
                        dbg["matched_agents"] = matched_dbg[:10]
                    return {
                        "candidate": candidate,
                        "ready": ready_count,
                        "maxStatusDuration": max_duration,
                        "waitingTooLong": waiting_too_long,
                        "debug": dbg
                    }

                # ---- COUNTS FALLBACK MODE (no egress or failure) ----
                idle_resp = await tld_ready(client, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "que": 0, "qui": "ing", "ava": 1, "sta": "false" if st else "true"
                })
                idle_now = False
                if "queue" in idle_resp:
                    try:
                        idle_now = int(idle_resp["queue"]) == 0 and int(idle_resp.get("ready", 0)) >= 1
                    except Exception:
                        idle_now = False
                if not idle_now:
                    idle_now = str(idle_resp.get("val", "0")).lower() in ("1", "true")

                rk = route_key(ING_THIS, st, phone)
                now = time.time()
                if idle_now:
                    idle_since.setdefault(rk, now)
                else:
                    idle_since.pop(rk, None)
                idle_age = int(now - idle_since[rk]) if rk in idle_since else 0
                waiting_too_long = idle_age >= IDLE_THRESHOLD_THIS
                candidate = bool(ready_ge_min or waiting_too_long)

                dbg = {
                    "mode": "counts_fallback",
                    "ing": ING_THIS,
                    "state": (st or "BASE"),
                    "route_key": rk,
                    "ready_min": READY_MIN_THIS,
                    "threshold": IDLE_THRESHOLD_THIS,
                    "ready_ge_min": ready_ge_min,
                    "idle_now": idle_now,
                }
                if raw:
                    dbg["counts_raw"] = counts
                    dbg["idle_probe_raw"] = idle_resp

                return {
                    "candidate": candidate,
                    "ready": ready_count,
                    "maxStatusDuration": idle_age,
                    "waitingTooLong": waiting_too_long,
                    "debug": dbg
                }

            # Evaluate requested states
            per_state: Dict[str, Dict[str, Any]] = {}
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
                "idleObservedSeconds": overall_maxdur,
                "debug": {
                    "ing": ING_THIS,
                    "states_evaluated": list(per_state.keys()),
                    "per_state": per_state,
                    "computed_should_accept": computed_should_accept,
                    "dry_mode": bool(dry),
                }
            }
            return resp

    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })
