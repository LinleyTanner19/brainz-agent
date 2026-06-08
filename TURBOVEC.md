# BRAINZ Search Layer — Upgrade Path & Benchmark Guide

Version: 2.2.0 | Updated: 2026-06-08 | Owner: Storeez / BRAINZ

## Overview

BRAINZ search has three phases that activate automatically as the Commons grows.
No code changes are needed to move between phases — only environment variables.
The benchmark endpoint (GET /benchmark) tracks current phase and tells you when to act.

---

## Phase Map

| Phase | Node Count | Embeddings | Primary Search | Action Required |
|-------|-----------|------------|----------------|------------------|
| **0** | < 200 | No | tsvector fulltext | None — default state |
| **1** | 200 – 5000 | Yes | TurboVec + pgvector | Set `EMBED_PROVIDER_KEY` |
| **2** | 5000+ | Yes | TurboVec (IVFFlat optimal) | None — auto-activates |

Every `/graph/query` response includes `search_mode` and `latency_ms` so you can see
exactly which tier fired on each query.

---

## Phase 0 — Bootstrap (Current State)

**Active today. Zero dependencies beyond Supabase.**

Search uses Postgres `tsvector` via the `search_nodes_fts` RPC function,
backed by a GIN index on `title + summary + content`. Works well at any node
count below ~200 with diverse vocabulary.

**What works right now on all 3 instances without any changes:**
- POST /sync/push → syncs node, generates Claude Haiku summary, stores in Supabase
- POST /graph/query → returns relevant nodes via fulltext keyword matching
- GET /benchmark → shows phase 0 status and growth recommendation
- TurboVec index is loaded at startup but reports 0 nodes (dormant, not broken)

**When to move to Phase 1:** When `GET /benchmark` reports
`nodes_until_next_phase: 0` (crossed 200 nodes), or when keyword search
starts returning poor results for conceptual queries.

---

## Phase 1 — Semantic Search

**Trigger: Set `EMBED_PROVIDER_KEY` in environment. No code changes. No rebuild.**

### Supported Providers

| Provider | EMBED_PROVIDER_KEY | EMBED_PROVIDER_URL | EMBED_MODEL | Cost/1M tokens |
|----------|-------------------|-------------------|-------------|----------------|
| OpenAI (default) | `sk-...` | _(blank)_ | `text-embedding-3-small` | $0.02 |
| OpenRouter | `sk-or-...` | `https://openrouter.ai/api/v1` | any compatible | varies |
| Voyage AI | `pa-...` | `https://api.voyageai.com/v1` | `voyage-3-lite` | $0.02 |

All three use the same OpenAI-compatible API. Switch providers by changing
`EMBED_PROVIDER_URL` — no code changes.

**Cost reality at current scale:**
- 1 node = ~$0.000002 (negligible)
- 200 nodes = ~$0.0004 total (one-off, write-once)
- 5000 nodes = ~$0.01 total

Embeddings are generated **once per node** (on create/update). Not on every query.

### Activation Checklist (Per Instance)

**Cloud (Railway):**
1. Railway dashboard → Variables → add `EMBED_PROVIDER_KEY=sk-...`
2. Optionally add `EMBED_PROVIDER_URL` if using OpenRouter/Voyage
3. Railway auto-redeploys on variable change
4. Watch logs: `Embedding provider active: model=text-embedding-3-small`
5. Run backfill script (see below) to embed existing nodes
6. `POST /vector/rebuild` to load embeddings into TurboVec

**Workstation:**
```
cd ~/.hermes/shared/brainz/railway
echo "EMBED_PROVIDER_KEY=sk-..." >> .env
docker compose up -d --force-recreate brainz-agent
docker compose logs -f brainz-agent
```

**Laptop:**
```
BRAINZ_NODE=laptop EMBED_PROVIDER_KEY=sk-... docker compose up -d --force-recreate brainz-agent
```

### After Activation: Verify Search Mode Shifted

```bash
curl -s -X POST https://companion.storeez.studio/graph/query \
  -H "Authorization: Bearer $BRAINZ_AUTH_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"vertical":"universal","question":"insurance innovation","top_k":5}' \
  | python3 -m json.tool | grep search_mode
```

Expected: `"search_mode": "turbovec"` (or `"pgvector"` while TurboVec is warming up).

---

## Phase 2 — TurboVec Optimal

**Auto-activates at 5000+ embedded nodes. No action required.**

At this scale:
- TurboVec ANN: < 5ms (in-process, no network)
- pgvector: ~200ms (Supabase EU round-trip)
- Index memory: ~4 MB for 10k nodes × 1536-dim at 4-bit

If pgvector queries appear slow after a bulk import, run in Supabase SQL editor:
```sql
ANALYZE commons_nodes;
```

---

## Benchmark Endpoint

`GET /benchmark` (auth required) — compare across all 3 instances.

### Example Response (Phase 0)

```json
{
  "phase": 0,
  "phase_label": "Bootstrap - fulltext search active",
  "node_count": 12,
  "embedded_count": 0,
  "embedding_coverage_pct": 0.0,
  "search_capability": "fulltext",
  "next_phase_at": 200,
  "nodes_until_next_phase": 188,
  "recommendation": "Fulltext search active, zero API dependencies. Next review at 200 nodes (188 to go). Keep enriching Commons.",
  "turbovec": {
    "available": true,
    "loaded": true,
    "node_count": 0,
    "active": false,
    "embed_dim": 1536,
    "embed_enabled": false
  },
  "embed_provider": {
    "configured": false,
    "model": null,
    "custom_url": false,
    "env_var": "EMBED_PROVIDER_KEY"
  },
  "thresholds": {
    "phase_0_max": 200,
    "phase_1_max": 5000,
    "phase_1_trigger": "Set EMBED_PROVIDER_KEY env var - no code changes needed",
    "phase_2_trigger": "Auto-activates when turbovec index reaches 5000+ embedded nodes"
  },
  "session_search_stats": {
    "turbovec": 0,
    "fulltext": 14,
    "recency": 0,
    "total_queries": 14,
    "mode_distribution": {
      "turbovec": 0.0,
      "fulltext": 100.0,
      "recency": 0.0
    }
  },
  "instance": "workstation"
}
```

### Decision Checklist When Reviewing Benchmark

- `phase: 0` and `nodes_until_next_phase: 0` → consider activating Phase 1
- `search_mode_distribution.fulltext < 80%` → fulltext degrading, activate Phase 1
- `phase: 1` and `turbovec.active: false` → run backfill + POST /vector/rebuild
- `phase: 2` and latency_ms > 100 → run ANALYZE commons_nodes in Supabase

---

## Backfill Script (Phase 1 activation only)

Run once after setting `EMBED_PROVIDER_KEY` to embed all existing nodes.

```python
import os, requests

VAULT   = os.path.expanduser("~/storeez-vault")
URL     = os.environ["BRAINZ_URL"]
TOKEN   = os.environ["BRAINZ_AUTH_SECRET"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

synced = 0
for root, _, files in os.walk(os.path.join(VAULT, "wiki/commons")):
    for fn in files:
        if not fn.endswith(".md"):
            continue
        abs_path = os.path.join(root, fn)
        rel_path = os.path.relpath(abs_path, VAULT)
        content  = open(abs_path).read()
        r = requests.post(
            f"{URL}/sync/push",
            json={"file_path": rel_path, "content": content, "source": "backfill"},
            headers=HEADERS,
            timeout=30,
        )
        synced += 1
        print(f"[{synced}] {r.status_code} {rel_path}")

print(f"Done: {synced} nodes re-synced with embeddings")
```

After backfill, reload TurboVec on each running instance:
```bash
curl -X POST $BRAINZ_URL/vector/rebuild -H "Authorization: Bearer $BRAINZ_AUTH_SECRET"
```

---

## Environment Variables

| Variable | Required | Phase | Purpose |
|----------|----------|-------|--------|
| `SUPABASE_URL` | Yes | All | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Yes | All | Supabase service role key |
| `BRAINZ_AUTH_SECRET` | Yes | All | Bearer token for API auth |
| `ANTHROPIC_API_KEY` | Yes | All | Claude Haiku for summaries |
| `BRAINZ_NODE` | Recommended | All | Instance label (workstation/laptop/cloud) |
| `EMBED_PROVIDER_KEY` | Phase 1+ | 1, 2 | Embedding API key (OpenAI/OpenRouter/Voyage) |
| `EMBED_PROVIDER_URL` | Optional | 1, 2 | Base URL for non-OpenAI providers |
| `EMBED_MODEL` | Optional | 1, 2 | Override embedding model (default: text-embedding-3-small) |
| `EMBED_DIM` | Optional | 1, 2 | Override embedding dimension (default: 1536) |

---

## Per-Instance Quick Reference

### Cloud — Railway
- URL: `https://companion.storeez.studio`
- Deploy: automatic on Railway variable changes
- Health: `curl https://companion.storeez.studio/health`
- Benchmark: `curl https://companion.storeez.studio/benchmark -H "Authorization: Bearer $BRAINZ_AUTH_SECRET"`

### Workstation (WSL / mac-studio)
- URL: `http://localhost:8000`
- Start: `docker compose up -d brainz-agent` from `~/.hermes/shared/brainz/railway/`
- Logs: `docker compose logs -f brainz-agent`
- BRAINZ_NODE: `workstation` (default)

### Laptop
- URL: `http://localhost:8000`
- Same docker-compose setup as workstation
- Set `BRAINZ_NODE=laptop` in `.env` so sync_log tracks which node pushed each file
- Can point vault watchdog at local instance (offline) or cloud (always latest):
  - Offline: `BRAINZ_URL=http://localhost:8000`
  - Online: `BRAINZ_URL=https://companion.storeez.studio`

Each instance maintains its own in-memory TurboVec index, seeded from shared Supabase.
No cross-instance sync needed. Supabase is the source of truth.

---

## Supabase Migrations Applied

| Migration | Date | Description |
|-----------|------|-------------|
| `commons_nodes_ivfflat_index` | 2026-06-08 | IVFFlat cosine index on embedding column |
| `search_nodes_fts_function` | 2026-06-08 | tsvector FTS RPC + GIN index (Phase 0 search) |

---

## Troubleshooting

**`search_mode: recency` on every query (Phase 0)**
FTS search returned no results — query too short or no keyword match.
Expected for empty Commons. As nodes accumulate, fulltext will activate.

**`search_mode: fulltext` but results feel semantic (expected in Phase 0)**
tsvector does partial semantic matching via stemming. Good enough until Phase 1.

**`search_mode: turbovec` not appearing after setting EMBED_PROVIDER_KEY**
1. Check `GET /vector/status` — if `loaded: true, node_count: 0`: run backfill script
2. Check startup logs: `Embedding provider active: ...`
3. If not in logs: verify `EMBED_PROVIDER_KEY` is set in the running container

**TurboVec import error in logs**
Rust not in PATH or pip install turbovec failed. Check Dockerfile build logs.
The service still works — falls back to fulltext search.

**Memory usage**
1k nodes × 1536 dim × 0.5 bytes (4-bit) ≈ 750 KB RAM.
10k nodes ≈ 7.5 MB. Negligible on any instance.

---

## API Reference

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | /health | No | Service health, node count, turbovec status |
| GET | /stats | Yes | Nodes per vertical, embedding coverage |
| GET | /benchmark | Yes | Phase tracker, growth metrics, recommendation |
| POST | /sync/push | Yes | Sync a Commons node from vault watchdog |
| POST | /graph/query | Yes | 4-tier semantic/fulltext search |
| GET | /context/{vertical} | Yes | Get cached microagent for a vertical |
| GET | /vector/status | Yes | TurboVec index status |
| POST | /vector/rebuild | Yes | Reload TurboVec from Supabase |
