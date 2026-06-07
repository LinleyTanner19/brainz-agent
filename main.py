import os, re
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
import anthropic

app = FastAPI(title="BRAINZ Agent", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
AUTH_SECRET   = os.environ["BRAINZ_AUTH_SECRET"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ai = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

VERTICALS = ["traveltech","instech","healthtech","fashion-web3","automotive","fintech","hardware-ai","universal"]

def require_auth(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "): raise HTTPException(401, "Missing Bearer token")
    token = authorization.split(" ", 1)[1]
    if token == AUTH_SECRET: return token
    result = supabase.table("tenants").select("id").eq("api_key", token).execute()
    if not result.data: raise HTTPException(403, "Invalid token")
    return token

class SyncPushPayload(BaseModel):
    file_path: str
    content: str
    source: str = "mac-studio"

class GraphQueryPayload(BaseModel):
    vertical: str
    question: str
    top_k: int = 8

@app.get("/health")
def health():
    nc = supabase.table("commons_nodes").select("id", count="exact").execute()
    return {"status": "alive", "service": "brainz-agent", "version": "2.0.0",
            "total_nodes": nc.count, "timestamp": datetime.utcnow().isoformat()}

@app.get("/stats")
def stats(_=Depends(require_auth)):
    result = supabase.table("commons_nodes").select("tenant_id").execute()
    by_tenant = {}
    for row in (result.data or []):
        t = row["tenant_id"]; by_tenant[t] = by_tenant.get(t, 0) + 1
    return {"nodes_by_vertical": by_tenant, "total": sum(by_tenant.values())}

@app.post("/sync/push")
def sync_push(payload: SyncPushPayload, _=Depends(require_auth)):
    parts = payload.file_path.replace("\\","/").split("/")
    tenant_id = "universal"
    if "commons" in parts:
        idx = parts.index("commons")
        if idx+1 < len(parts) and parts[idx+1] in VERTICALS: tenant_id = parts[idx+1]
    title_m = re.search(r"^# (.+)$", payload.content, re.MULTILINE)
    title = title_m.group(1).strip() if title_m else os.path.basename(payload.file_path).replace(".md","").title()
    surface_m = re.search(r"^surface: (.+)$", payload.content, re.MULTILINE)
    surface = surface_m.group(1).strip() if surface_m else (parts[-2] if len(parts)>2 else "general")
    val_m = re.search(r"^validated: (.+)$", payload.content, re.MULTILINE)
    validated = val_m.group(1).strip() if val_m else "hypothesis"
    try:
        resp = ai.messages.create(model="claude-haiku-4-5", max_tokens=120,
            messages=[{"role":"user","content":"Summarise in 1-2 sentences:\n\n"+payload.content[:3000]}])
        summary = resp.content[0].text.strip()
    except: summary = title
    nd = {"tenant_id":tenant_id,"file_path":payload.file_path,"title":title,
          "surface":surface,"content":payload.content[:10000],"summary":summary,
          "validated":validated,"updated_at":datetime.utcnow().isoformat()}
    res = supabase.table("commons_nodes").upsert(nd,on_conflict="tenant_id,file_path").execute()
    node_id = res.data[0]["id"]
    supabase.table("sync_log").insert({"tenant_id":tenant_id,"file_path":payload.file_path,
        "event_type":"updated","node_id":node_id,"source":payload.source}).execute()
    _regenerate_microagent(tenant_id)
    return {"status":"synced","node_id":node_id,"tenant":tenant_id,"title":title,"microagent_rebuilt":True}

@app.post("/graph/query")
def graph_query(payload: GraphQueryPayload, _=Depends(require_auth)):
    res = supabase.table("commons_nodes").select("id,title,surface,summary,tags,validated,updated_at").in_("tenant_id",[payload.vertical,"universal"]).order("updated_at",desc=True).limit(payload.top_k).execute()
    blocks = []
    for n in res.data:
        e = supabase.table("commons_edges").select("to_node").eq("from_node",n["id"]).limit(5).execute()
        rids = [x["to_node"] for x in (e.data or [])]
        rt = [r["title"] for r in supabase.table("commons_nodes").select("title").in_("id",rids).execute().data] if rids else []
        blocks.append({"title":n["title"],"surface":n["surface"],"summary":n["summary"],
            "tags":n.get("tags",[]),"validated":n["validated"],"related":rt})
    return {"vertical":payload.vertical,"question":payload.question,"node_count":len(blocks),"context":blocks}

@app.get("/context/{vertical}")
def get_context(vertical: str, _=Depends(require_auth)):
    cache = supabase.table("microagent_cache").select("content,node_count,generated_at").eq("tenant_id",vertical).execute()
    if not cache.data:
        _regenerate_microagent(vertical)
        cache = supabase.table("microagent_cache").select("content,node_count,generated_at").eq("tenant_id",vertical).execute()
    return {"vertical":vertical,"microagent":cache.data[0] if cache.data else None}

def _regenerate_microagent(tenant_id: str):
    nodes = supabase.table("commons_nodes").select("title,surface,summary,tags,validated").in_("tenant_id",[tenant_id,"universal"]).order("updated_at",desc=True).limit(200).execute()
    if not nodes.data: return
    by_surface = {}
    for n in nodes.data:
        s = n.get("surface") or "general"
        by_surface.setdefault(s,[]).append(n)
    total = len(nodes.data)
    val_count = sum(1 for n in nodes.data if n.get("validated")=="validated")
    header = ["---","name: "+tenant_id,"type: knowledge","agent: BRAINZ","triggers:",
              "  - "+tenant_id,"  - "+tenant_id.replace("-"," "),"---","",
              "# BRAINZ Intelligence - "+tenant_id.replace("-"," ").title(),
              "*"+str(total)+" patterns, "+str(val_count)+" validated - auto-generated*",""]
    body = []
    for surface, snodes in sorted(by_surface.items()):
        body.append("## "+surface.replace("-"," ").title())
        for n in snodes[:15]:
            v = " [validated]" if n.get("validated")=="validated" else ""
            body.append("- **"+n["title"]+"**"+v+": "+(n.get("summary") or ""))
        body.append("")
    footer = ["*Edit patterns in wiki/commons/ - changes sync automatically.*"]
    content = chr(10).join(header + body + footer)
    supabase.table("microagent_cache").upsert({"tenant_id":tenant_id,"content":content,
        "node_count":total,"generated_at":datetime.utcnow().isoformat()}).execute()
