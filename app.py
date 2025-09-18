import os, time, re
from typing import Dict, Any, Optional, List, Tuple
from fastapi import FastAPI, HTTPException, Query
import httpx

# ---- Configuration (env with sane defaults)
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")
READY_MIN      = int(os.getenv("READY_MIN", "2"))
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "60"))     # seconds
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "2.0"))

# Egress auth (for /api/egress/tldialer/*)
TLD_API_ID   = os.getenv("TLD_API_ID")
TLD_API_KEY  = os.getenv("TLD_API_KEY")

# Live-agents endpoint discovery
READYLIKE_STATUSES = {"closer", "ready"}  # ONLY these count
ENV_AGENT_STATUS_PATH = (os.getenv("AGENT_STATUS_PATH") or "").strip() or None
AGENT_ENDPOINT_CANDIDATES = [
    # We will try env-provided path first; then candidates below.
    # Public candidates (no auth)
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

# Counts/idle fallback state
idle_since: Dict[str, float] = {}

app = FastAPI(title="CallGrid Acceptance Mediator")

# ---------- helpers

def area_code_from_phone(p: str) -> str:
    m = re.search(r"(\d{10})$", re.sub(r"\D", "", p or ""))
    return (m.group(1)[0:3] if m else "UNK")

def route_key(ing_base: str, st: Optional[str], phone: str) -> str:
    return f"{ing_base}{(st or '')}|{area_code_from_phone(phone)}"

def normalize_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for k in ("agents", "data", "rows", "results"):
            v = data.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
    return []

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

def match_ingroup_state(row: Dict[str, Any], ing_base: str, st: Optional[str]) -> bool:
    ing_fields = [
        # vicidial_live_agents:
        "call_campaign_id",           # e.g. SREZMEDI_FL
        "call_ingroup_group_name",    # e.g. SR EZMed Inbound  FL

        # UI/other payloads we already tried:
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
        # exact coded id (SREZMEDI_FL) OR human label that ends with " FL"
        return (val_up == f"{ing_up}{st_up}") or val_up.endswith(f" {st_up}")
    # base only
    return val_up == ing_up or val_up.startswith(ing_up)


def row_status(row: Dict[str, Any]) -> str:
    s = pick(row, [
        "live_status",              # <-- vicidial_live_agents
        "Live Status", "status", "Status", "agent status"
    ]) or ""
    return str(s).strip().lower()


def row_status_duration_seconds(row: Dict[str, Any]) -> Optional[int]:
    dur = pick(row, [
        "last_state_duration",      # <-- vicidial_live_agents
        "Status Duration","status duration","duration","status_duration","live status duration"
    ])
    return parse_hhmmss_to_seconds(dur)


def auth_headers_for(path: str) -> Dict[str, str]:
    """
    Add the TLD API headers for tldialer egress endpoints.
    These header names are lowercased by TLD internally, but we send as shown.
    """
    if path and path.startswith("/api/egress/tldialer/") and TLD_API_ID and TLD_API_KEY:
        return {"tld-api-id": TLD_API_ID, "tld-api-key": TLD_API_KEY}
    return {}

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

async def discover_agents_endpoint(client: httpx.AsyncClient) -> Optional[str]:
    """
    Try env-provided path first (with egress headers if applicable), then public candidates.
    Cache on first 200 OK with list-shaped JSON.
    """
    global _agent_path_cache
    if _agent_path_cache and _agent_path_cache[1] > time.time():
        return _agent_path_cache[0]

    candidates: List[str] = []
    if ENV_AGENT_STATUS_PATH:
        candidates.append(ENV_AGENT_STATUS_PATH)
    candidates.extend([p for p in AGENT_ENDPOINT_CANDIDATES if p != ENV_AGENT_STATUS_PATH])

    for path in candidates:
        try:
            url = f"{TLD_BASE}{path}"
            r = await client.get(url, timeout=HTTP_TIMEOUT, headers=auth_headers_for(path))
            if r.status_code == 200:
                rows = normalize_rows(r.json())
                if isinstance(rows, list):  # shape looks good
                    _agent_path_cache = (path, time.time() + AGENT_PATH_CACHE_TTL)
                    return path
        except httpx.HTTPError:
            continue
    return None  # enables fallback

async def tld_agents_live(client: httpx.AsyncClient, path: str) -> List[Dict[str, Any]]:
    url = f"{TLD_BASE}{path}"
    r = await client.get(url, timeout=HTTP_TIMEOUT, headers=auth_headers_for(path))
    r.raise_for_status()
    return normalize_rows(r.json())

# ---------- Routes

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/diag/agents-endpoints")
async def diag_agents_endpoints():
    path = _agent_path_cache[0] if _agent_path_cache else None
    return {
        "current_path": path,
        "have_egress_auth": bool(TLD_API_ID and TLD_API_KEY),
        "candidates": ([ENV_AGENT_STATUS_PATH] if ENV_AGENT_STATUS_PATH else []) + AGENT_ENDPOINT_CANDIDATES
    }

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
      - ready_count >= ready_min
      - OR (agents mode): any agent with status in {CLOSER, READY} has StatusDuration >= threshold
      - OR (counts fallback): ava>=1 & queue==0 for ing+state for >= threshold seconds
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
            agents_path = await discover_agents_endpoint(client)
            mode = "agents" if agents_path else "counts_fallback"

            async def eval_state(st: Optional[str]) -> Dict[str, Any]:
                # ready counts
                counts = await tld_ready(client, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "sta": "false" if st else "true"
                })
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                ready_ge_min = ready_count >= READY_MIN_THIS

                if agents_path:
                    # AGENTS MODE
                    rows = await tld_agents_live(client, agents_path)
                    max_duration = 0
                    matched: List[Dict[str, Any]] = []
                    for row in rows:
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
                                    "ingroupField": pick(row, [
                                        "Call Ingroup Name","ingroup","Call Campaign / Ingroup","Campaign / Ingroup",
                                        "campaign","vendor","call_ingroup_group_name","call_campaign_id","campaign_id"
                                    ]),
                                    "agent": pick(row, ["Agent Full Name","agent_full_name","User","user","agent"]),

                                })
                        except Exception:
                            continue
                    waiting_too_long = (max_duration >= IDLE_THRESHOLD_THIS) and (max_duration > 0)
                    candidate = bool(ready_ge_min or waiting_too_long)
                    dbg = {
                        "mode": mode,
                        "ing": ING_THIS,
                        "state": (st or "BASE"),
                        "route_key": route_key(ING_THIS, st, phone),
                        "ready_min": READY_MIN_THIS,
                        "threshold": IDLE_THRESHOLD_THIS,
                        "ready_ge_min": ready_ge_min,
                        "agents_endpoint": agents_path,
                    }
                    if raw:
                        dbg["matched_agents"] = matched[:10]
                        dbg["counts_raw"] = counts
                    return {
                        "candidate": candidate,
                        "ready": ready_count,
                        "maxStatusDuration": max_duration,
                        "waitingTooLong": waiting_too_long,
                        "debug": dbg
                    }
                else:
                    # COUNTS FALLBACK MODE — idle timer using que==0 & ava>=1
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
                        "mode": mode,
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
                        "maxStatusDuration": idle_age,   # in fallback this reflects observed idle seconds
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
                    "ing": (ing or ING_BASE).strip(),
                    "states_evaluated": list(per_state.keys()),
                    "per_state": per_state,
                    "computed_should_accept": computed_should_accept,
                    "dry_mode": bool(dry),
                }
            }
            if agents_path:
                resp["debug"]["agents_endpoint_used"] = agents_path
            return resp

    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail={
            "shouldAccept": False,
            "error": f"TLD call failed: {str(e)}"
        })
