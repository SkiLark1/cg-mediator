import os, time, re
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
import httpx
import yaml

# =============================
# Global defaults (env + legacy)
# =============================
TLD_BASE       = os.getenv("TLD_BASE", "https://5star.tldcrm.com")
ING_BASE       = os.getenv("ING_BASE", "SRMEDTI_")
READY_MIN      = int(os.getenv("READY_MIN", "2"))
IDLE_THRESHOLD = int(os.getenv("IDLE_THRESHOLD", "60"))  # seconds
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "2.0"))

# Egress auth (legacy, used if tenant doesn't override)
TLD_API_ID  = os.getenv("TLD_API_ID")
TLD_API_KEY = os.getenv("TLD_API_KEY")

# If set, allow mapping a campaign → ingroup base when the row is missing ingroup fields (common for CLOSER)
ALLOW_CAMPAIGN_FALLBACK_ENV = os.getenv("ALLOW_CAMPAIGN_FALLBACK", "0") not in ("0", "", "false", "False")

# Legacy/global mapping (tenant can override)
CAMPAIGN_TO_ING_DEFAULT: Dict[str, str] = {
    "EZSALES": "SREZMEDI_",
    "MTSALES": "SRMEDTI_",
    "10":      "SRAHI_",     # Five Star Sales (Anchor/“SRAHI_”)
}

# Statuses that count toward "duration-based acceptance"
READYLIKE_STATUSES = {"closer", "ready"}

# Agent endpoint discovery (tenant can pin exact path)
ENV_AGENT_STATUS_PATH = (os.getenv("AGENT_STATUS_PATH") or "").strip() or None
AGENT_ENDPOINT_CANDIDATES = [
    "/api/egress/tldialer/vicidial_live_agents",  # preferred (needs egress auth)
    # public legacy candidates (kept for portability)
    "/api/public/dialer/live-agents",
    "/api/public/dialer/live_agents",
    "/api/public/dialer/agents",
    "/api/public/agents/live",
    "/api/public/agents",
    "/api/public/report/live_agents",
    "/api/public/report/agents/live",
]
# cache per-base: { base_url: (path, expires_at) }
_agent_path_cache: Dict[str, Tuple[str, float]] = {}
AGENT_PATH_CACHE_TTL = 300.0  # seconds

# Counts/idle/ready fallback state (keyed by route_key)
idle_since: Dict[str, float]  = {}
ready_since: Dict[str, float] = {}

# =============================
# Multi-tenant config (YAML)
# =============================
CONFIG_PATH = os.getenv("TENANTS_CONFIG", "/etc/cg-mediator/tenants.yaml")
TENANTS: Dict[str, Any] = {}
ING_PREFIX_TO_TENANT: Dict[str, str] = {}

def _load_tenants() -> None:
    global TENANTS, ING_PREFIX_TO_TENANT
    TENANTS = {}
    ING_PREFIX_TO_TENANT = {}
    p = Path(CONFIG_PATH)
    if p.exists():
        try:
            with p.open("r") as f:
                data = yaml.safe_load(f) or {}
            TENANTS = data.get("tenants", {}) or {}
            for tname, tinfo in TENANTS.items():
                for pref in (tinfo.get("ing_prefixes") or []):
                    ING_PREFIX_TO_TENANT[str(pref).upper()] = tname
        except Exception:
            # if config is bad, run with zero tenants (fallback to env)
            TENANTS = {}
            ING_PREFIX_TO_TENANT = {}

_load_tenants()

def pick_tenant(ing_param: Optional[str], explicit_tenant: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    """
    Choose a tenant:
      1) If 'tenant' query param is present: pick by name
      2) Else infer by ingroup base prefix (e.g. SREZMEDI_)
      3) Else fallback: single tenant if only one exists
      4) Else: env-only (empty tenant dict)
    """
    if explicit_tenant:
        t = TENANTS.get(explicit_tenant)
        if not t:
            raise HTTPException(status_code=400, detail=f"unknown tenant '{explicit_tenant}'")
        return explicit_tenant, t

    if ing_param:
        up = ing_param.upper()
        for pref, tname in ING_PREFIX_TO_TENANT.items():
            if up.startswith(pref):
                return tname, TENANTS[tname]

    if len(TENANTS) == 1:
        tname = next(iter(TENANTS.keys()))
        return tname, TENANTS[tname]

    # env-only fallback (legacy single-tenant)
    return "_env_", {}

# =============================
# FastAPI app
# =============================
app = FastAPI(title="CallGrid Acceptance Mediator (Multi-tenant)")

# =============
# Helper funcs
# =============
def area_code_from_phone(p: str) -> str:
    m = re.search(r"(\d{10})$", re.sub(r"\D", "", p or ""))
    return (m.group(1)[0:3] if m else "UNK")

def route_key(ing_base: str, st: Optional[str], phone: str) -> str:
    return f"{ing_base}{(st or '')}|{area_code_from_phone(phone)}"

def normalize_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for k in ("results", "agents", "data", "rows", "response"):
            v = data.get(k)
            # TLD egress often wraps as {"response": {"results": [...]}}
            if isinstance(v, dict) and "results" in v:
                rv = v.get("results")
                if isinstance(rv, list):
                    return [r for r in rv if isinstance(r, dict)]
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

def pick(obj: Dict[str, Any], keys: List[str], lower_ok: bool = True) -> Any:
    for k in keys:
        if k in obj:
            return obj[k]
    if lower_ok:
        low = {str(k).lower(): v for k, v in obj.items()}
        for k in keys:
            lk = k.lower()
            if lk in low:
                return low[lk]
    return None

def collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def suffix_after_underscore(s: str) -> Optional[str]:
    """SREZMEDI_FL → FL"""
    if not s:
        return None
    m = re.search(r"_([A-Z]{2})$", str(s).upper())
    return m.group(1) if m else None

def row_status(row: Dict[str, Any]) -> str:
    s = pick(row, ["live_status", "Live Status", "status", "Status", "agent status"]) or ""
    return str(s).strip().lower()

def row_status_duration_seconds(row: Dict[str, Any]) -> Optional[int]:
    dur = pick(row, [
        "last_state_duration",  # vicidial_live_agents
        "Status Duration", "status duration", "duration", "status_duration", "live status duration"
    ])
    return parse_hhmmss_to_seconds(dur)

def auth_headers_for(path: str, tld_api_id: Optional[str], tld_api_key: Optional[str]) -> Dict[str, str]:
    if path and path.startswith("/api/egress/tldialer/") and tld_api_id and tld_api_key:
        return {"tld-api-id": tld_api_id, "tld-api-key": tld_api_key}
    return {}

def row_matches_ing_state(
    row: Dict[str, Any],
    ing_base: str,
    st: Optional[str],
    allow_campaign_fallback: bool,
    campaign_to_ing: Dict[str, str]
) -> bool:
    """
    True if this agent row is associated with the requested ingroup+state.
    First try explicit ingroup fields; if missing and allowed, fall back to campaign→ingroup mapping.
    """
    ing_up = ing_base.upper()
    st_up  = (st or "").upper()

    # explicit ingroup via code (SREZMEDI_FL) or human label ("SR EZMed Inbound   FL")
    code    = pick(row, ["call_campaign_id"]) or ""
    label   = pick(row, ["call_ingroup_group_name"]) or ""
    label_c = collapse_spaces(str(label))
    code_up   = str(code).upper()
    label_up  = label_c.upper()

    if st:
        if code_up == f"{ing_up}{st_up}":
            return True
        if label_up.endswith(f" {st_up}"):
            return True
    else:
        if code_up.startswith(ing_up) or label_up.startswith(ing_up):
            return True

    # campaign → ingroup fallback (helps for CLOSER rows where ingroup fields are blank)
    if allow_campaign_fallback:
        campaign = (pick(row, ["campaign_id"]) or "").upper()
        mapped   = campaign_to_ing.get(campaign)
        if mapped and mapped.upper() == ing_up:
            state_from_code  = suffix_after_underscore(code_up)
            state_from_label = suffix_after_underscore(label_up.replace(" ", "_"))
            visible_state = state_from_code or state_from_label
            if st:
                if visible_state:
                    return visible_state == st_up
                return True  # no visible state; caller asked specific state → allow
            return True

    return False

# ===========
# TLD calls
# ===========
async def tld_ready(client: httpx.AsyncClient, base_url: str, phone: str, extra: Dict[str, Any]) -> Dict[str, Any]:
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
    url = f"{base_url}/api/public/dialer/ready/{phone}"
    r = await client.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

async def discover_agents_endpoint(
    client: httpx.AsyncClient,
    base_url: str,
    preferred_path: Optional[str],
    tld_api_id: Optional[str],
    tld_api_key: Optional[str]
) -> Optional[str]:
    now = time.time()
    cached = _agent_path_cache.get(base_url)
    if cached and cached[1] > now:
        return cached[0]

    candidates: List[str] = []
    if preferred_path:
        candidates.append(preferred_path)
    if ENV_AGENT_STATUS_PATH and ENV_AGENT_STATUS_PATH not in candidates:
        candidates.append(ENV_AGENT_STATUS_PATH)
    for p in AGENT_ENDPOINT_CANDIDATES:
        if p not in candidates:
            candidates.append(p)

    for path in candidates:
        try:
            url = f"{base_url}{path}"
            r = await client.get(url, timeout=HTTP_TIMEOUT, headers=auth_headers_for(path, tld_api_id, tld_api_key))
            if r.status_code == 200:
                rows = normalize_rows(r.json())
                if isinstance(rows, list):
                    _agent_path_cache[base_url] = (path, now + AGENT_PATH_CACHE_TTL)
                    return path
        except httpx.HTTPError:
            continue
    return None

async def tld_agents_live(client: httpx.AsyncClient, base_url: str, path: str,
                          tld_api_id: Optional[str], tld_api_key: Optional[str]) -> List[Dict[str, Any]]:
    url = f"{base_url}{path}"
    r = await client.get(url, timeout=HTTP_TIMEOUT, headers=auth_headers_for(path, tld_api_id, tld_api_key))
    r.raise_for_status()
    return normalize_rows(r.json())

# ===========
# Routes
# ===========
@app.get("/healthz")
async def healthz():
    return {"ok": True, "tenants": list(TENANTS.keys()) or ["_env_"]}

@app.get("/diag/agents-endpoints")
async def diag_agents_endpoints(tenant: Optional[str] = Query(None), ing: Optional[str] = Query(None)):
    # pick tenant context for discovery (but don't trigger network here)
    _, TENANT = pick_tenant(ing, tenant)
    base      = (TENANT.get("tld_base") if TENANT else TLD_BASE)
    preferred = (TENANT.get("agent_status_path") if TENANT else None) or ENV_AGENT_STATUS_PATH
    have_egress_auth = bool((TENANT.get("tld_api_id") if TENANT else TLD_API_ID) and
                            (TENANT.get("tld_api_key") if TENANT else TLD_API_KEY))
    cached = _agent_path_cache.get(base)
    return {
        "tenant": tenant or None,
        "current_path": cached[0] if cached else None,
        "have_egress_auth": have_egress_auth,
        "campaign_fallback": bool(TENANT.get("allow_campaign_fallback", ALLOW_CAMPAIGN_FALLBACK_ENV)),
        "base_url": base,
        "preferred": preferred,
        "candidates": [c for c in ([preferred] if preferred else []) + AGENT_ENDPOINT_CANDIDATES],
    }

@app.get("/accept")
async def accept(
    phone: str = Query(..., min_length=7, max_length=25),
    ing: Optional[str] = Query(None, description="Ingroup base, e.g. SREZMEDI_ or SRMEDTI_"),
    ready_min: Optional[int] = Query(None, description="Override READY_MIN"),
    threshold: Optional[int] = Query(None, description="Override IDLE_THRESHOLD (seconds)"),
    dry: Optional[int] = Query(0, description="If 1, never accept (safe test)"),
    state: Optional[str] = Query(None, description="Single state (e.g., FL)"),
    states: Optional[str] = Query(None, description="CSV states (e.g., MI,TX,OK)"),
    raw: Optional[int] = Query(0, description="If 1, include raw debug"),
    tenant: Optional[str] = Query(None, description="Explicit tenant name from YAML")
):
    # Ingroup + thresholds
    ING_THIS = (ing or ING_BASE).strip()
    READY_MIN_THIS = READY_MIN if ready_min is None else int(ready_min)
    IDLE_THRESHOLD_THIS = IDLE_THRESHOLD if threshold is None else int(threshold)

    # Tenant context
    tenant_name, TENANT = pick_tenant(ING_THIS, tenant)
    TLD_BASE_THIS       = TENANT.get("tld_base", TLD_BASE)
    TLD_API_ID_THIS     = TENANT.get("tld_api_id", TLD_API_ID)
    TLD_API_KEY_THIS    = TENANT.get("tld_api_key", TLD_API_KEY)
    AGENT_PATH_PREFERRED= TENANT.get("agent_status_path") or ENV_AGENT_STATUS_PATH
    ALLOW_FALLBACK_THIS = bool(TENANT.get("allow_campaign_fallback", ALLOW_CAMPAIGN_FALLBACK_ENV))
    CAMPAIGN_TO_ING_THIS= TENANT.get("campaign_to_ing", CAMPAIGN_TO_ING_DEFAULT)

    # parse states
    if states:
        state_list: List[Optional[str]] = [s.strip().upper() for s in states.split(",") if s.strip()]
    elif state:
        state_list = [state.strip().upper()]
    else:
        state_list = [None]

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            agents_path = await discover_agents_endpoint(
                client, TLD_BASE_THIS, AGENT_PATH_PREFERRED, TLD_API_ID_THIS, TLD_API_KEY_THIS
            )  # may be None

            async def eval_state(st: Optional[str]) -> Dict[str, Any]:
                # ---- 1) COUNTS: always compute
                counts = await tld_ready(client, TLD_BASE_THIS, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "sta": "false" if st else "true"
                })
                ready_count = int(counts.get("ready", counts.get("ava", 0)) or 0)
                ready_ge_min = ready_count >= READY_MIN_THIS

                # probe idle now? (queue==0 & ready>=1)
                idle_probe = await tld_ready(client, TLD_BASE_THIS, phone, extra={
                    "ing": f"{ING_THIS}{st}" if st else ING_THIS,
                    "que": 0, "qui": "ing", "ava": 1, "sta": "false" if st else "true"
                })
                idle_now = False
                if "queue" in idle_probe:
                    try:
                        idle_now = int(idle_probe["queue"]) == 0 and int(idle_probe.get("ready", 0)) >= 1
                    except Exception:
                        idle_now = False
                if not idle_now:
                    idle_now = str(idle_probe.get("val", "0")).lower() in ("1", "true")

                rk = route_key(ING_THIS, st, phone)
                now = time.time()

                # idle timer
                if idle_now:
                    idle_since.setdefault(rk, now)
                else:
                    idle_since.pop(rk, None)
                idle_age = int(now - idle_since[rk]) if rk in idle_since else 0

                # ready timer — as long as at least 1 agent is READY in this ingroup+state
                if ready_count >= 1:
                    ready_since.setdefault(rk, now)
                else:
                    ready_since.pop(rk, None)
                ready_age = int(now - ready_since[rk]) if rk in ready_since else 0

                # baseline effective duration from counts: max of idle & ready ages
                effective_max_secs = max(idle_age, ready_age)
                source_mode = "counts_fallback"
                matched: List[Dict[str, Any]] = []

                # ---- 2) AGENTS overlay (if it actually shows READY/CLOSER)
                if agents_path:
                    try:
                        rows = await tld_agents_live(client, TLD_BASE_THIS, agents_path, TLD_API_ID_THIS, TLD_API_KEY_THIS)
                        max_ready_secs = 0
                        for row in rows:
                            status_l = row_status(row)
                            if status_l not in READYLIKE_STATUSES:
                                continue
                            if not row_matches_ing_state(row, ING_THIS, st, ALLOW_FALLBACK_THIS, CAMPAIGN_TO_ING_THIS):
                                continue
                            dur = row_status_duration_seconds(row)
                            if dur is None:
                                continue
                            d = int(dur)
                            if d > max_ready_secs:
                                max_ready_secs = d
                            if raw:
                                matched.append({
                                    "status": status_l,
                                    "duration": d,
                                    "agent": pick(row, ["agent_full_name","Agent Full Name","User","user","agent"]),
                                    "campaign": pick(row, ["campaign_id"]),
                                    "call_campaign_id": pick(row, ["call_campaign_id"]),
                                    "call_ingroup_group_name": pick(row, ["call_ingroup_group_name"]),
                                })
                        if max_ready_secs > 0:
                            effective_max_secs = max_ready_secs
                            source_mode = "agents"
                    except Exception:
                        pass  # keep counts result

                waiting_too_long = effective_max_secs >= IDLE_THRESHOLD_THIS
                candidate = bool(ready_ge_min or waiting_too_long)

                dbg = {
                    "mode": source_mode,
                    "tenant": tenant_name,
                    "ing": ING_THIS,
                    "state": (st or "BASE"),
                    "route_key": rk,
                    "ready_min": READY_MIN_THIS,
                    "threshold": IDLE_THRESHOLD_THIS,
                    "ready_ge_min": ready_ge_min,
                    "ready_count": ready_count,
                    "idle_now": idle_now,
                    "idle_age": idle_age,
                    "ready_age": ready_age,
                    "campaign_fallback": bool(ALLOW_FALLBACK_THIS),
                }
                if agents_path:
                    dbg["agents_endpoint"] = agents_path
                    if raw and matched:
                        dbg["matched_agents"] = matched[:10]
                if raw:
                    dbg["counts_raw"] = counts
                    dbg["idle_probe_raw"] = idle_probe

                return {
                    "candidate": candidate,
                    "ready": ready_count,
                    "maxStatusDuration": effective_max_secs,
                    "waitingTooLong": waiting_too_long,
                    "debug": dbg
                }

            # evaluate states
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
                    "tenant": tenant_name,
                    "ing": ING_THIS,
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
