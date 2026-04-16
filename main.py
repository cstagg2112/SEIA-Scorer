from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Request, Form, Cookie
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import sqlite3
import json
import io
import os
import httpx
from datetime import datetime
from typing import Optional
from scorer import score_firm, parse_fintrx_row
from close_crm import push_to_close, push_all_to_close

app = FastAPI(title="SEIA RIA M&A Scorer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

DB_PATH = "seia_scorer.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS firms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crd TEXT UNIQUE,
            name TEXT,
            city TEXT,
            state TEXT,
            aum_m REAL,
            total_accounts INTEGER,
            advisor_count INTEGER,
            classification TEXT,
            firm_type TEXT,
            activities TEXT,
            fee_structure TEXT,
            retail_custodian TEXT,
            cagr1 REAL,
            cagr3 REAL,
            cagr5 REAL,
            geo_tier INTEGER,
            composite INTEGER,
            tier TEXT,
            services_score INTEGER,
            services_label TEXT,
            services_demoted INTEGER,
            client_qual_score INTEGER,
            client_tier TEXT,
            client_tier_label TEXT,
            avg_acct TEXT,
            flags TEXT,
            dims TEXT,
            push_status TEXT DEFAULT 'pending',
            push_date TEXT,
            batch_id TEXT,
            scored_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id TEXT PRIMARY KEY,
            name TEXT,
            firm_count INTEGER,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()


init_db()


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("templates/index.html") as f:
        return f.read()


@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), sep=None, engine="python", dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {str(e)}")

    df.columns = [c.strip() for c in df.columns]
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_name = file.filename or batch_id

    firms = []
    errors = []
    for idx, row in df.iterrows():
        try:
            parsed = parse_fintrx_row(row)
            scored = score_firm(parsed)
            firms.append({**parsed, **scored, "batch_id": batch_id, "scored_at": datetime.now().isoformat()})
        except Exception as e:
            errors.append({"row": idx, "error": str(e)})

    if not firms:
        raise HTTPException(status_code=400, detail="No valid firms found in file")

    conn = get_db()
    inserted = 0
    updated = 0
    for f in firms:
        existing = conn.execute("SELECT id FROM firms WHERE crd = ?", (f["crd"],)).fetchone()
        flags_json = json.dumps(f.get("flags", []))
        dims_json = json.dumps(f.get("dims", {}))
        if existing:
            conn.execute("""
                UPDATE firms SET name=?, city=?, state=?, aum_m=?, total_accounts=?,
                advisor_count=?, classification=?, firm_type=?, activities=?, fee_structure=?,
                retail_custodian=?, cagr1=?, cagr3=?, cagr5=?, geo_tier=?, composite=?,
                tier=?, services_score=?, services_label=?, services_demoted=?,
                client_qual_score=?, client_tier=?, client_tier_label=?, avg_acct=?,
                flags=?, dims=?, batch_id=?, scored_at=?
                WHERE crd=?
            """, (
                f["name"], f["city"], f["state"], f["aum_m"], f["total_accounts"],
                f["advisor_count"], f["classification"], f["firm_type"], f["activities"],
                f["fee_structure"], f["retail_custodian"], f["cagr1"], f["cagr3"], f["cagr5"],
                f["geo_tier"], f["composite"], f["tier"], f["services_score"],
                f["services_label"], int(f["services_demoted"]), f["client_qual_score"],
                f["client_tier"], f["client_tier_label"], f["avg_acct"],
                flags_json, dims_json, f["batch_id"], f["scored_at"], f["crd"]
            ))
            updated += 1
        else:
            conn.execute("""
                INSERT INTO firms (crd, name, city, state, aum_m, total_accounts,
                advisor_count, classification, firm_type, activities, fee_structure,
                retail_custodian, cagr1, cagr3, cagr5, geo_tier, composite, tier,
                services_score, services_label, services_demoted, client_qual_score,
                client_tier, client_tier_label, avg_acct, flags, dims, batch_id, scored_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                f["crd"], f["name"], f["city"], f["state"], f["aum_m"], f["total_accounts"],
                f["advisor_count"], f["classification"], f["firm_type"], f["activities"],
                f["fee_structure"], f["retail_custodian"], f["cagr1"], f["cagr3"], f["cagr5"],
                f["geo_tier"], f["composite"], f["tier"], f["services_score"],
                f["services_label"], int(f["services_demoted"]), f["client_qual_score"],
                f["client_tier"], f["client_tier_label"], f["avg_acct"],
                flags_json, dims_json, f["batch_id"], f["scored_at"]
            ))
            inserted += 1

    conn.execute(
        "INSERT OR REPLACE INTO batches (id, name, firm_count, created_at) VALUES (?,?,?,?)",
        (batch_id, batch_name, len(firms), datetime.now().isoformat())
    )
    conn.commit()
    conn.close()

    return {
        "batch_id": batch_id,
        "total": len(firms),
        "inserted": inserted,
        "updated": updated,
        "errors": len(errors)
    }


@app.get("/api/firms")
async def get_firms(
    tier: Optional[str] = None,
    services: Optional[str] = None,
    client_type: Optional[str] = None,
    firm_type: Optional[str] = None,
    geo_tier: Optional[int] = None,
    search: Optional[str] = None,
    batch_id: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
    sort_by: str = "composite",
    sort_dir: str = "desc"
):
    conn = get_db()
    query = "SELECT * FROM firms WHERE 1=1"
    params = []

    if tier:
        query += " AND tier = ?"
        params.append(tier)
    if services:
        query += " AND services_label = ?"
        params.append(services)
    if client_type:
        query += " AND client_tier = ?"
        params.append(client_type)
    if firm_type == "independent":
        query += " AND firm_type = 'independent'"
    elif firm_type == "other":
        query += " AND firm_type != 'independent'"
    if geo_tier:
        query += " AND geo_tier = ?"
        params.append(geo_tier)
    if search:
        query += " AND (name LIKE ? OR crd LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    if batch_id:
        query += " AND batch_id = ?"
        params.append(batch_id)

    safe_cols = {"composite", "name", "aum_m", "tier", "scored_at"}
    sort_col = sort_by if sort_by in safe_cols else "composite"
    sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"

    count_row = conn.execute(f"SELECT COUNT(*) FROM firms WHERE 1=1" + query.split("WHERE 1=1")[1], params).fetchone()
    total = count_row[0]

    query += f" ORDER BY {sort_col} {sort_dir} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(query, params).fetchall()
    conn.close()

    firms = []
    for r in rows:
        f = dict(r)
        f["flags"] = json.loads(f["flags"] or "[]")
        f["dims"] = json.loads(f["dims"] or "{}")
        firms.append(f)

    return {"firms": firms, "total": total, "limit": limit, "offset": offset}


@app.get("/api/stats")
async def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM firms").fetchone()[0]
    tier_a = conn.execute("SELECT COUNT(*) FROM firms WHERE tier='A'").fetchone()[0]
    avg_score = conn.execute("SELECT AVG(composite) FROM firms").fetchone()[0]
    demoted = conn.execute("SELECT COUNT(*) FROM firms WHERE services_demoted=1").fetchone()[0]
    pushed = conn.execute("SELECT COUNT(*) FROM firms WHERE push_status='pushed'").fetchone()[0]
    batches = conn.execute("SELECT * FROM batches ORDER BY created_at DESC LIMIT 10").fetchall()
    conn.close()
    return {
        "total": total,
        "tier_a": tier_a,
        "avg_score": round(avg_score, 1) if avg_score else 0,
        "demoted": demoted,
        "pushed": pushed,
        "batches": [dict(b) for b in batches]
    }


@app.post("/api/push/{crd}")
async def push_firm(crd: str):
    conn = get_db()
    firm = conn.execute("SELECT * FROM firms WHERE crd=?", (crd,)).fetchone()
    if not firm:
        raise HTTPException(status_code=404, detail="Firm not found")
    firm = dict(firm)
    firm["flags"] = json.loads(firm["flags"] or "[]")
    firm["dims"] = json.loads(firm["dims"] or "{}")

    settings = {r["key"]: r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
    api_key = settings.get("close_api_key", "")

    if not api_key:
        raise HTTPException(status_code=400, detail="Close CRM API key not configured. Add it in Settings.")

    result = await push_to_close(firm, api_key)

    if result["success"]:
        conn.execute(
            "UPDATE firms SET push_status='pushed', push_date=? WHERE crd=?",
            (datetime.now().isoformat(), crd)
        )
        conn.commit()

    conn.close()
    return result


@app.post("/api/push-all")
async def push_all_firms(background_tasks: BackgroundTasks, tier: Optional[str] = None):
    conn = get_db()
    settings = {r["key"]: r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
    api_key = settings.get("close_api_key", "")
    if not api_key:
        raise HTTPException(status_code=400, detail="Close CRM API key not configured.")

    query = "SELECT * FROM firms WHERE push_status='pending'"
    params = []
    if tier:
        query += " AND tier=?"
        params.append(tier)

    firms = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    background_tasks.add_task(push_all_to_close, firms, api_key, DB_PATH)
    return {"queued": len(firms), "message": f"Pushing {len(firms)} firms to Close in background"}


@app.get("/api/settings")


@app.get("/api/states")
async def get_states(request: Request):
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT state FROM firms WHERE state IS NOT NULL AND state != '' ORDER BY state").fetchall()
    conn.close()
    return {"states": [r["state"] for r in rows]}

@app.get("/api/cities")
async def get_cities(request: Optional[str] = None, state: Optional[str] = None):
    conn = get_db()
    if state:
        rows = conn.execute("SELECT DISTINCT city FROM firms WHERE state=? AND city IS NOT NULL AND city != '' ORDER BY city", (state,)).fetchall()
    else:
        rows = conn.execute("SELECT DISTINCT city FROM firms WHERE city IS NOT NULL AND city != '' ORDER BY city").fetchall()
    conn.close()
    return {"cities": [r["city"] for r in rows]}

async def get_settings():
    conn = get_db()
    settings = {r["key"]: r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    if "close_api_key" in settings:
        key = settings["close_api_key"]
        settings["close_api_key"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
    return settings


@app.post("/api/settings")
async def save_settings(payload: dict):
    conn = get_db()
    for key, value in payload.items():
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)",
            (key, value)
        )
    conn.commit()
    conn.close()
    return {"saved": True}


@app.get("/api/export")
async def export_csv(
    tier: Optional[str] = None,
    services: Optional[str] = None,
    batch_id: Optional[str] = None
):
    conn = get_db()
    query = "SELECT * FROM firms WHERE 1=1"
    params = []
    if tier:
        query += " AND tier=?"
        params.append(tier)
    if services:
        query += " AND services_label=?"
        params.append(services)
    if batch_id:
        query += " AND batch_id=?"
        params.append(batch_id)
    query += " ORDER BY composite DESC"

    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    output = io.StringIO()
    output.write("CRD,Firm Name,City,State,AUM ($M),Total Accounts,Avg Account Size,Client Tier,Score,Tier,Services Fit,Geo Tier,Classification,Flags,Push Status,Batch,Scored At\n")
    for f in rows:
        flags = json.loads(f.get("flags") or "[]")
        flag_str = "|".join([fl.get("t","") if isinstance(fl,dict) else str(fl) for fl in flags])
        output.write(f'"{f["crd"]}","{f["name"]}","{f["city"]}","{f["state"]}",{round(f["aum_m"] or 0,1)},{f["total_accounts"] or 0},"{f["avg_acct"] or ""}","{f["client_tier_label"] or ""}",{f["composite"]},{f["tier"]},"{f["services_label"]}",{f["geo_tier"]},"{f["classification"] or ""}","{flag_str}","{f["push_status"]}","{f["batch_id"]}","{f["scored_at"]}"\n')

    from fastapi.responses import Response
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=SEIA_RIA_Scores.csv"}
    )


