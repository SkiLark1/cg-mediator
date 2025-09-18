import os, time, re
from typing import Dict, Any, Optional, List, Tuple
from fastapi import FastAPI, HTTPException, Query
import httpx

# ---- Configuration (env with sane defaults)
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")          # default ingroup base
READY_MIN      = int(os.getenv("READY_MIN", "2"))           # "more than 1 agent"
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "60"))     # seconds
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "2.0"))

# Live-agents endpoint discovery
READYLIKE_STATUSES = {"closer", "ready"}  # ONLY these count
ENV_AGENT_STATUS_PATH = (os.getenv("AGENT_STATUS_PATH") or "").strip() or None
AGENT_ENDPOINT_CANDIDATES = [
    "/api/public/dialer/live-agents",
    "/api/public/dialer/live_agents",
    "/api/public/dialer/agents",
    "/api/public/agents/live",
    "/api/public/agents",
    "/api/public/report/live_agents",
    "/api/public/report/agents/live",
]
_agent_path_cache: Optional[Tuple[str, float]] = None
AGENT_PATH_CACHE_TTL = 300.0  # seconds

app = FastAPI(title="CallGrid Acceptance Mediator")

# ---------- helpers

def area_code_from_phone(p: str) -> str:
    m = re.search(r"(\d{10})$", re.sub(r"\D", "", p or ""))
    return (m.group(1)[0:3] if m else "UNK")

def route_key_from_json(base: str, counts: Dict[str, Any], phone: str) -> str:
    st = str(counts.get("state") or counts.get("st") or area_code_from_phone(phone))
    ing = str(counts.get("ingroup") or counts.get("vendor") or base)
    return f"{ing}|{st}"

def normalize_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for k in ("agents", "data", "rows"):
            v = data.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
    return []

def parse_hhmmss_to_seconds(s: str) -> Optional[int]:
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

def match_ingroup_state(row: Dict[str, Any], ing_base: str, st: Optional[str]) -> bool:
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
    return val_up == ing_up or val_up.startswith(ing_up)

def row_status(row: Dict[str, Any]) -> str:
    s = pick(row, ["Live Status","status","Status","agent status"]) or ""
    return str(s).strip().lower()

def row_status_duration_seconds(row: Dict[str, Any]) -> Optional[int]:
    dur = pick(row, ["Status Duration","status duration","duration","status_duration","live status duration"])
    return parse_hhmmss_to_seconds(dur)

# ---------- TLD calls

async def tld_ready(client: httpx.AsyncClient, phone: str, extra: Dict[str, Any]) -> Dict[str, Any]:
    base_params = {
        "ava": 1,
        "ing": ING_BASE,   # may be overridden by extra["ing"]
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

async def discover_agents_endpoint(client: httpx.AsyncClient) -> str:
    """Find a working per-agent live endpoint and cache it."""
    global _agent_path_cache

    # use cache if fresh
    if _agent_path_cache and _agent_path_cache[1] > time.time():
        return _agent_path_cache[0]

    candidates = []
    if ENV_AGENT_STATUS_PATH:
        candidates.append(ENV_AGENT_STATUS_PATH)
    candidates.extend([p for p in AGENT_ENDPOINT_CANDIDATES if p != ENV_AGENT_STATUS_PATH])

    for path in candidates:
        try:
            url = f"{TLD_BASE}{path}"
            r = await client.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                rows = normalize_rows(r.json())
                if rows is not None:  # even empty list is acceptable; shape is OK
                    _agent_path_cache = (path, time.time() + AGENT_PATH_CACHE_TTL)
                    return path
        except httpx.HTTPError:
            continue

    raise HTTPException(status_code=502, detail={
        "shouldAccept": False,
        "error": "No per-agent Live endpoint found; set AGENT_STATUS_PATH env or contact TLD support."
    })

async def tld_agents_live(client: httpx.AsyncClient) -> Tuple[str, List[Dict[str, Any]]]:
    """Return (path_used, agent_rows)."""
    path = await discover_agents_endpoint(client)
    url = f"{TLD_BASE}{path}"
    r = await client.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    rows = normalize_rows(r.json())
    return path, rows

# ---------- Routes

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/diag/agents-endpoints")
async def diag_agents_endpoints():
    """Quick diag: which endpoint is discovered right now."""
    path = _agent_path_cache[0] if _agent_path_cache else None
    return {
        "current_path": path,
        "candidates": ([ENV_AGENT_STATUS_PATH] if ENV_AGENT_STATUS_PATH else []) + AGENT_ENDPOINT_CANDIDATES
    }

@app.get("/accept")
async def accept(
    phone: str = Query(..., min_length=7, max_length=25),
    ing: Optional[str] = Query(None, description="TLD ingroup base, e.g., SREZMEDI_ or SRMEDTI_"),
    ready_min: Optional[int] = Query(None, description="Override READY_MIN"),
    threshold: Optional[int] = Query(None, description="Override IDLE_THRESHOLD (seconds)"),
    dry: Optional[int] = Query(0, description="If 1, never accept (for safe testing)"),
    # state selection
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
    state_list: List[Optional[str]] = []
    if states:
        state_list = [s.strip().upper() for s in states.split(",") if s.strip()]
    elif state:
        state_list = [state.strip().upper()]
    else:
        state_list = [None]  # base only unless you pass states

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            # fetch agents once; we’ll filter locally
            agents_path, agents_rows = await tld_agents_live(client)

            async def eval_state(st: Optional[str]) -> Dict[str, Any]:
                # counts: fast path for "ready >= ready_min"
                counts = await tld_ready(client, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "sta": "false" if st else "true"  # when state forced, don't let TLD append again
                })
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                ready_ge_min = ready_count >= READY_MIN_THIS

                # from agent rows: only statuses closer/ready, compute max status duration
                max_duration = 0
                matched: List[Dict[str, Any]] = []
                for row in agents_rows:
                    try:
                        if not match_ingroup_state(row, ING_THIS, st):
                            continue
                        status_l = row_status(row)
                        if status_l not in READYLIKE_STATUSES:
                            continue
                        dur = row_status_duration_seconds(row)
                        if dur is None:
                            continue
                        max_duration = max(max_duration, int(dur))
                        if raw:
                            matched.append({
                                "status": status_l,
                                "duration": int(dur),
                                "ingroupField": pick(row, ["Call Ingroup Name","ingroup","Call Campaign / Ingroup",
                                                           "Campaign / Ingroup","campaign","vendor"]),
                                "agent": pick(row, ["Agent Full Name","User","user","agent"]),
                            })
                    except Exception:
                        continue

                waiting_too_long = (max_duration >= IDLE_THRESHOLD_THIS) and (max_duration > 0)
                candidate = bool(ready_ge_min or (waiting_too_long and max_duration > 0))

                return {
                    "candidate": candidate,
                    "ready": ready_count,
                    "maxStatusDuration": max_duration,
                    "waitingTooLong": waiting_too_long,
                    "debug": {
                        "ing": ING_THIS,
                        "state": (st or "BASE"),
                        "route_key": f"{ING_THIS}{st or ''}|{area_code_from_phone(phone)}",
                        "ready_min": READY_MIN_THIS,
                        "threshold": IDLE_THRESHOLD_THIS,
                        "ready_ge_min": ready_ge_min,
                        "agents_endpoint": agents_path,
                        **({"counts_raw": counts} if raw else {}),
                        **({"matched_agents": matched[:10]} if raw else {}),
                    }
                }

            # Evaluate all requested states
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
                "idleObservedSeconds": overall_maxdur,  # now = max agent Status Duration (sec)
                "debug": {
                    "ing": ING_THIS,
                    "states_evaluated": list(per_state.keys()),
                    "per_state": per_state,
                    "computed_should_accept": computed_should_accept,
                    "dry_mode": bool(dry),
                }
            }
            if raw:
                resp["debug"]["agents_endpoint_used"] = agents_path
                # Include a small sample of raw rows to verify field names
                resp["debug"]["agents_raw_sample"] = agents_rows[:10]

            return resp

    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })
