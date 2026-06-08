#!/usr/bin/env python3
"""BRAINZ Agent v2.3.0 - 4-tier search + BetterAuth session validation.

Auth chain (require_auth):
  1. BRAINZ_AUTH_SECRET  master token (machine-to-machine, always works)
  2. BetterAuth session  user/OAuth tokens validated via BETTER_AUTH_URL
  3. Tenant API key      legacy Supabase tenants table

Search chain: Phase 0 tsvector -> Phase 1 turbovec+pgvector -> Phase 2 turbovec optimal
Activate BetterAuth: set BETTER_AUTH_URL in env. No code changes needed.
Benchmark: GET /benchmark - phase, coverage, session stats.
Auth info: GET /auth/info - auth mode, betterauth reachability.
"""
import os, re, time, threading, hashlib, logging
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List
import numpy as np
import httpx
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
import anthropic

# Optional embedding support. Active only when EMBED_PROVIDER_KEY is set (Phase 1+).
# Supports OpenAI (default), Google Gemini, Voyage AI, or any OpenAI-compatible endpoint.
# EMBED_PROVIDER_URL: leave blank for OpenAI, set to provider base_url for others
#   (Gemini: https://generativelanguage.googleapis.com/v1beta/openai/).
# EMBED_MODEL: default text-embedding-3-small (1536-dim, matches commons_nodes.embedding).
#   Gemini: gemini-embedding-001 (defaults to 3072 — dimensions=EMBED_DIM truncates to 1536).
# EMBED_DIM is passed as `dimensions` on every call so any provider matches the 1536 column.
_embed_client = None
EMBED_MODEL = os.environ.get("EMBED_MODEL", "text-embedding-3-small")
EMBED_DIM = int(os.environ.get("EMBED_DIM", "1536"))
EMBED_KEY = os.environ.get("EMBED_PROVIDER_KEY", "")
EMBED_URL = os.environ.get("EMBED_PROVIDER_URL", "")

if EMBED_KEY:
    try:
        from openai import OpenAI as _OAI
        _kw = {"api_key": EMBED_KEY}
        if EMBED_URL:
            _kw["base_url"] = EMBED_URL
        _embed_client = _OAI(**_kw)
    except ImportError:
        pass

try:
    from turbovec import IdMapIndex as _TurboIdMap
    _TURBOVEC_OK = True
except ImportError:
    _TURBOVEC_OK = False

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
AUTH_SECRET = os.environ["BRAINZ_AUTH_SECRET"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
BRAINZ_NODE = os.environ.get("BRAINZ_NODE", "workstation")
# BetterAuth: cloud=http://betterauth:3001 (internal), workstation/laptop=https://auth.storeez.studio
BETTER_AUTH_URL = os.environ.get("BETTER_AUTH_URL", "").rstrip("/")

VERTICALS = [
    "traveltech", "instech", "healthtech", "fashion-web3",
    "automotive", "fintech", "hardware-ai", "universal",
]

PHASE_THRESHOLDS = {
    "phase_0_max": 200,
    "phase_1_max": 5000,
    "phase_1_trigger": "Set EMBED_PROVIDER_KEY env var - no code changes needed",
    "phase_2_trigger": "Auto-activates when turbovec index reaches 5000+ embedded nodes",
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("brainz")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

_search_stats: dict = defaultdict(int)
_http_client: Optional[httpx.AsyncClient] = None


def _embed(text: str) -> Optional[np.ndarray]:
    if not _embed_client:
        return None
    try:
        resp = _embed_client.embeddings.create(model=EMBED_MODEL, input=text[:8000], dimensions=EMBED_DIM)
        return np.array(resp.data[0].embedding, dtype=np.float32)
    except Exception as exc:
        logger.warning("embed error: %s", exc)
        return None


def _uuid_to_uint64(uuid_str: str) -> int:
    digest = hashlib.sha256(uuid_str.encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def _fts_search(query: str, tenants: List[str], k: int) -> List[str]:
    """ Postgres tsvector search via search_nodes_fts RPC. Zero API dependencies.
    Active in Phase 0 and as fallback in Phase 1+. Backed by GIN index."""
    try:
        res = supabase.rpc("search_nodes_fts", {
            "search_query": query,
            "filter_tenants": tenants,
            "match_count": k,
        }).execute()
        return [r["id"] for r in (res.data or [])]
    except Exception as exc:
        logger.warning("FTS search error: %s", exc)
        return []

class TurboVecLayer:
    def __init__(self):
        self._lock = threading.RLock()
        self._index = None
        self._id_map = {}
        self._vert_map = {}
        self._loaded = False
        self._count = 0
        self._init_index()

    def _init_index(self):
        if _TURBOVEC_OK:
            try:
                self._index = _TurboIdMap(dim=EMBED_DIM, bit_width=4)
            except Exception as exc:
                logger.warning("TurboVec init error: %s", exc)
                self._index = None

    def load_from_supabase(self):
        logger.info("TurboVec: scanning Supabase for embedded nodes ...")
        rows, page, size = [], 0, 500
        try:
            while True:
                res = (supabase.table("commons_nodes")
                       .select("id,tenant_id,embedding")
                       .not_.is_("embedding", "null")
                       .range(page * size, (page + 1) * size - 1)
                       .execute())
                if not res.data:
                    break
                rows.extend(res.data)
                if len(res.data) < size:
                    break
                page += 1
        except Exception as exc:
            logger.error("TurboVec load failed: %s", exc)
            self._loaded = True
            return

        if not rows:
            logger.info("TurboVec: 0 embedded nodes - fulltext search is active (Phase 0)")
            self._loaded = True
            return

        vecs, ids, id_map, vert_map = [], [], {}, {}
        for row in rows:
            emb = row.get("embedding")
            if emb is None:
                continue
            uid = _uuid_to_uint64(row["id"])
            vecs.append(np.array(emb, dtype=np.float32))
            ids.append(uid)
            id_map[uid] = row["id"]
            vert_map[uid] = row.get("tenant_id", "universal")

        if not vecs:
            self._loaded = True
            return

        matrix = np.stack(vecs, axis=0)
        arr = np.array(ids, dtype=np.uint64)
        with self._lock:
            self._init_index()
            if self._index is not None:
                self._index.add_with_ids(matrix, arr)
            self._id_map = id_map
            self._vert_map = vert_map
            self._count = len(vecs)
            self._loaded = True
            logger.info("TurboVec: loaded %d embedded nodes (4-bit, dim=%d)", self._count, EMBED_DIM)

    def add(self, uuid_str: str, tenant_id: str, embedding: np.ndarray):
        if not (_TURBOVEC_OK and self._index is not None):
            return
        uid = _uuid_to_uint64(uuid_str)
        with self._lock:
            if uid in self._id_map:
                try:
                    self._index.remove(uid)
                    self._count -= 1
                except Exception:
                    pass
            try:
                self._index.add_with_ids(embedding.reshape(1, -1), np.array([uid], dtype=np.uint64))
                self._id_map[uid] = uuid_str
                self._vert_map[uid] = tenant_id
                self._count += 1
            except Exception as exc:
                logger.warning("TurboVec add error: %s", exc)

    def search(self, query_vec: np.ndarray, k: int, vertical: Optional[str] = None) -> List[str]:
        if not (self._loaded and self._count > 0 and self._index is not None):
            return []
        with self._lock:
            try:
                filter_ids = None
                if vertical:
                    allowed = [uid for uid, v in self._vert_map.items()
                               if v in (vertical, "universal")]
                    if allowed:
                        filter_ids = np.array(allowed, dtype=np.uint64)
                actual_k = min(k, self._count)
                if filter_ids is not None:
                    scores, ids = self._index.search(query_vec.reshape(1, -1), k=actual_k, filter_ids=filter_ids)
                else:
                    scores, ids = self._index.search(query_vec.reshape(1, -1), k=actual_k)
                return [self._id_map[uid] for uid in ids[0] if uid in self._id_map]
            except Exception as exc:
                logger.warning("TurboVec search error: %s", exc)
                return []

    @property
    def status(self) -> dict:
        return {
            "available": _TURBOVEC_OK,
            "loaded": self._loaded,
            "node_count": self._count,
            "active": self._count > 0,
            "embed_dim": EMBED_DIM,
            "embed_enabled": _embed_client is not None,
        }


tv = TurboVecLayer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    _http_client = httpx.AsyncClient(timeout=5.0)
    t = threading.Thread(target=tv.load_from_supabase, daemon=True, name="turbovec-loader")
    t.start()
    if BETTER_AUTH_URL:
        logger.info("BetterAuth: validation via %s", BETTER_AUTH_URL)
    else:
        logger.info("BetterAuth: BETTER_AUTH_URL not set — master secret + tenant key auth only")
    yield
    await _http_client.aclose()


app = FastAPI(title="BRAINZ Agent", version="2.3.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


async def _validate_betterauth_session(token: str) -> Optional[dict]:
    """Call BetterAuth /api/auth/get-session. Returns session dict or None."""
    if not BETTER_AUTH_URL or not _http_client:
        return None
    try:
        resp = await _http_client.get(
            f"{BETTER_AUTH_URL}/api/auth/get-session",
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.status_code == 200:
            data = resp.json()
            if data and data.get("session"):
                return data
    except Exception as exc:
        logger.warning("BetterAuth validation error: %s", exc)
    return None


async def require_auth(authorization: str = Header(...)):
    """Auth chain: master secret -> BetterAuth session -> tenant API key."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    token = authorization.split(" ", 1)[1]
    # 1. Master secret (machine-to-machine, backward compat)
    if token == AUTH_SECRET:
        return {"type": "master"}
    # 2. BetterAuth session (user/OAuth — active when BETTER_AUTH_URL is set)
    if BETTER_AUTH_URL:
        session = await _validate_betterauth_session(token)
        if session:
            user = session.get("user", {})
            logger.debug("BetterAuth: authenticated %s", user.get("email", "unknown"))
            return {"type": "betterauth", "user": user}
    # 3. Tenant API key (backward compat)
    result = supabase.table("tenants").select("id").eq("api_key", token).execute()
    if result.data:
        return {"type": "tenant", "tenant_id": result.data[0]["id"]}
    raise HTTPException(403, "Invalid token")


class SyncPushPayload(BaseModel):
    file_path: str
    content: str
    source: str = "workstation"


class SyncDeletePayload(BaseModel):
    file_path: str
    source: str = "mac-studio"

class SyncRenamePayload(BaseModel):
    old_path: str
    new_path: str
    source: str = "mac-studio"

class GraphQueryPayload(BaseModel):
    vertical: str
    question: str
    top_k: int = 8

def _current_phase(node_count: int, embedded_count: int) -> dict:
    p0 = PHASE_THRESHOLDS["phase_0_max"]
    p1 = PHASE_THRESHOLDS["phase_1_max"]
    if embedded_count > 0 and node_count >= p1:
        phase, label = 2, "Semantic - TurboVec optimal range"
    elif embedded_count > 0:
        phase, label = 1, "Semantic - TurboVec active"
    elif node_count >= p0:
        phase, label = 1, "Ready for embeddings - threshold crossed"
    else:
        phase, label = 0, "Bootstrap - fulltext search active"

    next_at = p0 if phase == 0 else p1
    until = max(0, next_at - node_count)

    if phase == 0 and until > 0:
        rec = ("Fulltext search active, zero API dependencies. "
               "Next review at %d nodes (%d to go). Keep enriching Commons." % (p0, until))
    elif phase == 0:
        rec = ("Commons crossed %d-node threshold. "
               "Consider activating semantic search: set EMBED_PROVIDER_KEY. "
               "Fulltext still works well." % p0)
    elif phase == 1 and until > 0:
        rec = ("Semantic search active via TurboVec + pgvector. "
               "TurboVec optimal at %d nodes (%d to go)." % (p1, until))
    else:
        rec = ("TurboVec operating optimally at %d nodes. "
               "If pgvector queries slow: run ANALYZE commons_nodes." % node_count)

    return {
        "phase": phase,
        "phase_label": label,
        "next_phase_at": next_at,
        "nodes_until_next_phase": until,
        "recommendation": rec,
    }


@app.get("/")
def root():
    """Root landing — lists all available endpoints."""
    return {
        "service": "BRAINZ Companion",
        "version": "2.3.0",
        "node": BRAINZ_NODE,
        "status": "alive",
        "docs": "/docs",
        "endpoints": {
            "health":         "GET  /health              - status, node count, turbovec phase",
            "stats":          "GET  /stats               - nodes per vertical, embed coverage",
            "benchmark":      "GET  /benchmark           - search phase tracker, upgrade guide",
            "auth_info":      "GET  /auth/info           - auth modes, BetterAuth reachability",
            "sync_push":      "POST /sync/push           - ingest a commons node",
            "graph_query":    "POST /graph/query         - semantic search across commons",
            "context":        "GET  /context/{vertical}  - retrieve context for a vertical",
            "vector_status":  "GET  /vector/status       - TurboVec index status",
            "vector_rebuild": "POST /vector/rebuild      - force TurboVec index rebuild",
        },
        "auth": "Authorization: Bearer <BRAINZ_AUTH_SECRET>  (required on all except / and /health)",
    }


@app.get("/health")
def health():
    nc = supabase.table("commons_nodes").select("id", count="exact").execute()
    return {
        "status": "alive", "service": "brainz-agent", "version": "2.3.0",
        "total_nodes": nc.count, "turbovec": tv.status,
        "node": BRAINZ_NODE, "timestamp": datetime.utcnow().isoformat(),
        "auth": {"betterauth_configured": bool(BETTER_AUTH_URL), "betterauth_url": BETTER_AUTH_URL or None},
    }


@app.get("/auth/info")
async def auth_info(_=Depends(require_auth)):
    """Auth configuration and reachability for this instance."""
    betterauth_ok = False
    if BETTER_AUTH_URL and _http_client:
        try:
            r = await _http_client.get(f"{BETTER_AUTH_URL}/api/auth/ok", timeout=3.0)
            betterauth_ok = r.status_code == 200
        except Exception:
            betterauth_ok = False
    return {
        "instance": BRAINZ_NODE,
        "auth_modes": {
            "master_secret": True,
            "betterauth": bool(BETTER_AUTH_URL),
            "tenant_api_key": True,
        },
        "betterauth": {
            "configured": bool(BETTER_AUTH_URL),
            "url": BETTER_AUTH_URL or None,
            "reachable": betterauth_ok,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/stats")
async def stats(_=Depends(require_auth)):
    result = supabase.table("commons_nodes").select("tenant_id").execute()
    by_tenant: dict = {}
    for row in result.data or []:
        t = row["tenant_id"]
        by_tenant[t] = by_tenant.get(t, 0) + 1
    embedded = (supabase.table("commons_nodes")
                .select("id", count="exact").not_.is_("embedding", "null").execute())
    return {
        "nodes_by_vertical": by_tenant,
        "total": sum(by_tenant.values()),
        "embedded_nodes": embedded.count,
        "turbovec": tv.status,
    }


@app.get("/benchmark")
async def benchmark(_=Depends(require_auth)):
    """Phase tracker: Commons growth, embedding coverage, search mode distribution.
    Use to decide when to add EMBED_PROVIDER_KEY (Phase 0->1) or verify TurboVec
    is in optimal range (Phase 2). Compare /benchmark across instances:
    node_count/embedded_count are consistent (Supabase), session_search_stats are
    per-instance in-memory (reset on restart)."""
    nc_total = supabase.table("commons_nodes").select("id", count="exact").execute()
    nc_embed = (supabase.table("commons_nodes")
                .select("id", count="exact").not_.is_("embedding", "null").execute())
    total = nc_total.count or 0
    embedded = nc_embed.count or 0
    coverage = round(embedded / total * 100, 1) if total > 0 else 0.0
    phase_info = _current_phase(total, embedded)
    search_cap = "turbovec" if tv.status["active"] else ("fulltext" if total > 0 else "recency")
    tq = _search_stats["total"]
    return {
        **phase_info,
        "node_count": total,
        "embedded_count": embedded,
        "embedding_coverage_pct": coverage,
        "search_capability": search_cap,
        "turbovec": tv.status,
        "embed_provider": {
            "configured": _embed_client is not None,
            "model": EMBED_MODEL if _embed_client else None,
            "custom_url": bool(EMBED_URL),
            "env_var": "EMBED_PROVIDER_KEY",
        },
        "thresholds": PHASE_THRESHOLDS,
        "session_search_stats": {
            "turbovec": _search_stats["turbovec"],
            "fulltext": _search_stats["fulltext"],
            "recency": _search_stats["recency"],
            "total_queries": tq,
            "mode_distribution": {
                k: (round(v / tq * 100, 1) if tq else 0.0)
                for k, v in [
                    ("turbovec", _search_stats["turbovec"]),
                    ("fulltext", _search_stats["fulltext"]),
                    ("recency", _search_stats["recency"]),
                ]
            },
        },
        "instance": BRAINZ_NODE,
        "timestamp": datetime.utcnow().isoformat(),
    }

@app.post("/sync/push")
async def sync_push(payload: SyncPushPayload, background: BackgroundTasks, _=Depends(require_auth)):
    parts = payload.file_path.replace("\\", "/").split("/")
    tenant_id = "universal"
    if "commons" in parts:
        idx = parts.index("commons")
        if idx + 1 < len(parts) and parts[idx + 1] in VERTICALS:
            tenant_id = parts[idx + 1]
    title_m = re.search(r"^# (.+)$", payload.content, re.MULTILINE)
    title = (title_m.group(1).strip() if title_m else
             os.path.basename(payload.file_path).replace(".md", "").title())
    surface_m = re.search(r"^surface: (.+)$", payload.content, re.MULTILINE)
    surface = (surface_m.group(1).strip() if surface_m else
               (parts[-2] if len(parts) > 2 else "general"))
    val_m = re.search(r"^validated: (.+)$", payload.content, re.MULTILINE)
    validated = val_m.group(1).strip() if val_m else "hypothesis"
    try:
        resp = ai.messages.create(
            model="claude-haiku-4-5", max_tokens=120,
            messages=[{"role": "user",
                       "content": "Summarise in 1-2 sentences:\n\n" + payload.content[:3000]}])
        summary = resp.content[0].text.strip()
    except Exception:
        summary = title
    embed_text = title + ". " + summary + ". " + payload.content[:500]
    embedding = _embed(embed_text)
    nd: dict = {
        "tenant_id": tenant_id, "file_path": payload.file_path,
        "title": title, "surface": surface, "content": payload.content[:10000],
        "summary": summary, "validated": validated,
        "updated_at": datetime.utcnow().isoformat(),
    }
    if embedding is not None:
        nd["embedding"] = embedding.tolist()
    res = supabase.table("commons_nodes").upsert(nd, on_conflict="tenant_id,file_path").execute()
    node_id = res.data[0]["id"]
    supabase.table("sync_log").insert({
        "tenant_id": tenant_id, "file_path": payload.file_path,
        "event_type": "updated", "node_id": node_id,
        "source": payload.source or BRAINZ_NODE,
    }).execute()
    if embedding is not None:
        background.add_task(tv.add, node_id, tenant_id, embedding)
    background.add_task(_regenerate_microagent, tenant_id)
    return {
        "status": "synced", "node_id": node_id, "tenant": tenant_id,
        "title": title, "embedded": embedding is not None, "microagent_rebuilt": True,
    }

@app.post("/sync/delete")
def sync_delete(payload: SyncDeletePayload, _=Depends(require_auth)):
    res = supabase.table("commons_nodes").select("id,tenant_id").eq("file_path",payload.file_path).eq("is_active",True).limit(1).execute()
    if not res.data: raise HTTPException(404, "No active node for file_path='"+payload.file_path+"'")
    node_id = res.data[0]["id"]; tenant_id = res.data[0]["tenant_id"]
    ts = datetime.utcnow().isoformat()
    supabase.table("commons_nodes").update({"deleted_at":ts,"is_active":False,"updated_at":ts}).eq("id",node_id).execute()
    # commons_edges uses from_node / to_node
    supabase.table("commons_edges").update({"is_stale":True}).or_("from_node.eq."+node_id+",to_node.eq."+node_id).execute()
    supabase.table("sync_log").insert({"tenant_id":tenant_id,"file_path":payload.file_path,
        "event_type":"deleted","node_id":node_id,"source":payload.source}).execute()
    _regenerate_microagent(tenant_id)
    return {"status":"deleted","node_id":node_id,"tenant":tenant_id}

@app.post("/sync/rename")
def sync_rename(payload: SyncRenamePayload, _=Depends(require_auth)):
    res = supabase.table("commons_nodes").select("id,tenant_id").eq("file_path",payload.old_path).eq("is_active",True).limit(1).execute()
    if not res.data: raise HTTPException(404, "No active node for old_path='"+payload.old_path+"'")
    node_id = res.data[0]["id"]; tenant_id = res.data[0]["tenant_id"]
    conflict = supabase.table("commons_nodes").select("id").eq("file_path",payload.new_path).eq("is_active",True).limit(1).execute()
    if conflict.data: raise HTTPException(409, "Active node already exists at new_path='"+payload.new_path+"'")
    new_title = os.path.basename(payload.new_path).replace(".md","").title()
    ts = datetime.utcnow().isoformat()
    supabase.table("commons_nodes").update({"file_path":payload.new_path,"title":new_title,"updated_at":ts}).eq("id",node_id).execute()
    supabase.table("sync_log").insert({"tenant_id":tenant_id,"file_path":payload.new_path,
        "event_type":"renamed","node_id":node_id,"source":payload.source}).execute()
    return {"status":"renamed","node_id":node_id,"old_path":payload.old_path,"new_path":payload.new_path}

@app.post("/graph/query")
async def graph_query(payload: GraphQueryPayload, _=Depends(require_auth)):
    """4-tier search chain.
    Tier 1 TurboVec ANN: requires EMBED_PROVIDER_KEY + embedded nodes.
    Tier 2 pgvector RPC: requires EMBED_PROVIDER_KEY, TurboVec warming.
    Tier 3 tsvector fulltext: zero dependencies, always available.
    Tier 4 recency: safety net, no text matching."""
    t0 = time.monotonic()
    search_mode = "recency"
    node_ids: List[str] = []
    query_vec = _embed(payload.question)
    if query_vec is not None:
        tv_ids = tv.search(query_vec, k=payload.top_k, vertical=payload.vertical)
        if tv_ids:
            node_ids = tv_ids
            search_mode = "turbovec"
    if not node_ids and query_vec is not None:
        try:
            pg_res = supabase.rpc("match_nodes", {
                "query_embedding": query_vec.tolist(),
                "filter_tenants": [payload.vertical, "universal"],
                "match_count": payload.top_k,
                "similarity_floor": 0.3,
            }).execute()
            if pg_res.data:
                node_ids = [r["id"] for r in pg_res.data]
                search_mode = "pgvector"
        except Exception as exc:
            logger.warning("pgvector fallback error: %s", exc)
    if not node_ids:
        fts_ids = _fts_search(payload.question, [payload.vertical, "universal"], payload.top_k)
        if fts_ids:
            node_ids = fts_ids
            search_mode = "fulltext"
    if not node_ids:
        res = (supabase.table("commons_nodes").select("id")
               .in_("tenant_id", [payload.vertical, "universal"])
               .order("updated_at", desc=True).limit(payload.top_k).execute())
        node_ids = [r["id"] for r in (res.data or [])]
        search_mode = "recency"
    _search_stats[search_mode] += 1
    _search_stats["total"] += 1
    latency_ms = round((time.monotonic() - t0) * 1000, 1)
    if not node_ids:
        return {
            "vertical": payload.vertical, "question": payload.question,
            "node_count": 0, "context": [], "search_mode": search_mode,
            "latency_ms": latency_ms,
        }
    full = (supabase.table("commons_nodes")
            .select("id,title,surface,summary,tags,validated,updated_at")
            .in_("id", node_ids).execute())
    order = {nid: i for i, nid in enumerate(node_ids)}
    nodes = sorted(full.data or [], key=lambda n: order.get(n["id"], 999))
    blocks = []
    for n in nodes:
        edges = supabase.table("commons_edges").select("to_node").eq("from_node", n["id"]).limit(5).execute()
        rids = [x["to_node"] for x in (edges.data or [])]
        rt = ([r["title"] for r in supabase.table("commons_nodes").select("title").in_("id", rids).execute().data]
              if rids else [])
        blocks.append({
            "title": n["title"], "surface": n["surface"], "summary": n["summary"],
            "tags": n.get("tags", []), "validated": n["validated"], "related": rt,
        })
    return {
        "vertical": payload.vertical, "question": payload.question,
        "node_count": len(blocks), "context": blocks,
        "search_mode": search_mode, "latency_ms": latency_ms,
    }


@app.get("/context/{vertical}")
async def get_context(vertical: str, _=Depends(require_auth)):
    cache = (supabase.table("microagent_cache")
             .select("content,node_count,generated_at").eq("tenant_id", vertical).execute())
    if not cache.data:
        _regenerate_microagent(vertical)
        cache = (supabase.table("microagent_cache")
                 .select("content,node_count,generated_at").eq("tenant_id", vertical).execute())
    return {"vertical": vertical, "microagent": cache.data[0] if cache.data else None}


@app.post("/vector/rebuild")
async def vector_rebuild(background: BackgroundTasks, _=Depends(require_auth)):
    """Reload TurboVec index from Supabase. Run after bulk Commons imports."""
    background.add_task(tv.load_from_supabase)
    return {"status": "rebuild_queued", "current": tv.status}


@app.get("/vector/status")
async def vector_status(_=Depends(require_auth)):
    return {"turbovec": tv.status, "timestamp": datetime.utcnow().isoformat()}

def _regenerate_microagent(tenant_id: str):
    nodes = (supabase.table("commons_nodes")
             .select("title,surface,summary,tags,validated")
             .in_("tenant_id", [tenant_id, "universal"])
             .order("updated_at", desc=True).limit(200).execute())
    if not nodes.data:
        return
    by_surface: dict = {}
    for n in nodes.data:
        s = n.get("surface") or "general"
        by_surface.setdefault(s, []).append(n)
    total = len(nodes.data)
    val_count = sum(1 for n in nodes.data if n.get("validated") == "validated")
    header = [
        "---", "name: " + tenant_id, "type: knowledge", "agent: BRAINZ",
        "triggers:", "  - " + tenant_id, "  - " + tenant_id.replace("-", " "),
        "---", "",
        "BRAINZ Intelligence - " + tenant_id.replace("-", " ").title(),
        "*%d patterns, %d validated - auto-generated*" % (total, val_count), "",
    ]
    body: list = []
    for surface, snodes in sorted(by_surface.items()):
        body.append("## " + surface.replace("-", " ").title())
        for n in snodes[:15]:
            v = " [validated]" if n.get("validated") == "validated" else ""
            body.append("- **" + n["title"] + "**" + v + ": " + (n.get("summary") or ""))
        body.append("")
    footer = ["*Edit patterns in wiki/commons/ - changes sync automatically.*"]
    content = "\n".join(header + body + footer)
    supabase.table("microagent_cache").upsert({
        "tenant_id": tenant_id, "content": content,
        "node_count": total, "generated_at": datetime.utcnow().isoformat(),
    }).execute()
