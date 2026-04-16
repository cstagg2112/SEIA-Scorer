from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Request, Form, Cookie
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import sqlite3
import json
import io
import os
from datetime import datetime
from typing import Optional
from scorer import score_firm, parse_fintrx_row
from close_crm import push_to_close, push_all_to_close
from auth import make_token, verify_token, login_page, SESSION_TOKENS, APP_PASSWORD

app = FastAPI(title="SEIA RIA M&A Scorer")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")
DB_PATH = os.environ.get("DB_PATH", "seia_scorer.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""CREATE TABLE IF NOT EXISTS firms (id INTEGER PRIMARY KEY AUTOINCREMENT, crd TEXT UNIQUE, name TEXT, city TEXT, state TEXT, aum_m REAL, total_accounts INTEGER, advisor_count INTEGER, classification TEXT, firm_type TEXT, activities TEXT, fee_structure TEXT, retail_custodian TEXT, cagr1 REAL, cagr3 REAL, cagr5 REAL, geo_tier INTEGER, nearest_metro TEXT, dist_metro_miles REAL, nearest_seia TEXT, dist_seia_miles REAL, geo_label TEXT, composite INTEGER, tier TEXT, services_score INTEGER, services_label TEXT, services_demoted INTEGER, client_qual_score INTEGER, client_tier TEXT, client_tier_label TEXT, avg_acct TEXT, flags TEXT, dims TEXT, push_status TEXT DEFAULT 'pending', push_date TEXT, batch_id TEXT, scored_at TEXT, manual_entry INTEGER DEFAULT 0)""")
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS batches (id TEXT PRIMARY KEY, name TEXT, firm_count INTEGER, created_at TEXT)")
    for col_sql in ["ALTER TABLE firms ADD COLUMN nearest_metro TEXT","ALTER TABLE firms ADD COLUMN dist_metro_miles REAL","ALTER TABLE firms ADD COLUMN nearest_seia TEXT","ALTER TABLE firms ADD COLUMN dist_seia_miles REAL","ALTER TABLE firms ADD COLUMN geo_label TEXT","ALTER TABLE firms ADD COLUMN manual_entry INTEGER DEFAULT 0"]:
        try: conn.execute(col_sql)
        except: pass
    conn.commit(); conn.close()

init_db()

def check_auth(request: Request):
    token = request.cookies.get("auth_token")
    if not token or not verify_token(token):
        raise HTTPException(status_code=401, detail="Unauthorized")

def upsert_firm(conn, f, batch_id):
    flags_json=json.dumps(f.get("flags",[])); dims_json=json.dumps(f.get("dims",{}))
    existing=conn.execute("SELECT id FROM firms WHERE crd=?",(f["crd"],)).fetchone()
    vals=(f["name"],f["city"],f["state"],f["aum_m"],f["total_accounts"],f["advisor_count"],f.get("classification",""),f.get("firm_type",""),f.get("activities",""),f.get("fee_structure",""),f.get("retail_custodian",""),f.get("cagr1"),f.get("cagr3"),f.get("cagr5"),f.get("geo_tier",3),f.get("nearest_metro"),f.get("dist_metro_miles"),f.get("nearest_seia"),f.get("dist_seia_miles"),f.get("geo_label"),f["composite"],f["tier"],f["services_score"],f["services_label"],int(f["services_demoted"]),f["client_qual_score"],f["client_tier"],f["client_tier_label"],f.get("avg_acct"),flags_json,dims_json,batch_id,datetime.now().isoformat(),int(f.get("manual_entry",0)))
    if existing:
        conn.execute("UPDATE firms SET name=?,city=?,state=?,aum_m=?,total_accounts=?,advisor_count=?,classification=?,firm_type=?,activities=?,fee_structure=?,retail_custodian=?,cagr1=?,cagr3=?,cagr5=?,geo_tier=?,nearest_metro=?,dist_metro_miles=?,nearest_seia=?,dist_seia_miles=?,geo_label=?,composite=?,tier=?,services_score=?,services_label=?,services_demoted=?,client_qual_score=?,client_tier=?,client_tier_label=?,avg_acct=?,flags=?,dims=?,batch_id=?,scored_at=?,manual_entry=? WHERE crd=?",(*vals,f["crd"])); return "updated"
    else:
        conn.execute("INSERT INTO firms (name,city,state,aum_m,total_accounts,advisor_count,classification,firm_type,activities,fee_structure,retail_custodian,cagr1,cagr3,cagr5,geo_tier,nearest_metro,dist_metro_miles,nearest_seia,dist_seia_miles,geo_label,composite,tier,services_score,services_label,services_demoted,client_qual_score,client_tier,client_tier_label,avg_acct,flags,dims,batch_id,scored_at,manual_entry,crd) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(*vals,f["crd"])); return "inserted"

@app.get("/login", response_class=HTMLResponse)
async def login_get():
    return login_page()

@app.post("/login")
async def login_post(password: str = Form(...)):
    if password == APP_PASSWORD:
        token=make_token(); SESSION_TOKENS.add(token)
        resp=RedirectResponse(url="/",status_code=302)
        resp.set_cookie("auth_token",token,httponly=True,samesite="lax",max_age=86400*7)
        return resp
    return login_page(error=True)

@app.post("/logout")
async def logout(request: Request):
    token=request.cookies.get("auth_token")
    if token: SESSION_TOKENS.discard(token)
    resp=RedirectResponse(url="/login",status_code=302)
    resp.delete_cookie("auth_token"); return resp

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    token=request.cookies.get("auth_token")
    if not token or not verify_token(token): return RedirectResponse(url="/login")
    with open("templates/index.html") as f: return f.read()

@app.post("/api/upload")
async def upload_csv(request: Request, file: UploadFile = File(...)):
    check_auth(request)
    content=await file.read()
    try: df=pd.read_csv(io.BytesIO(content),sep=None,engine="python",dtype=str)
    except Exception as e: raise HTTPException(status_code=400,detail=f"Could not parse file: {str(e)}")
    df.columns=[c.strip() for c in df.columns]
    batch_id=datetime.now().strftime("%Y%m%d_%H%M%S"); batch_name=file.filename or batch_id
    firms,errors=[],[]
    for idx,row in df.iterrows():
        try:
            parsed=parse_fintrx_row(row)
            if not parsed["name"]: continue
            scored=score_firm(parsed); firms.append({**parsed,**scored})
        except Exception as e: errors.append({"row":idx,"error":str(e)})
    if not firms: raise HTTPException(status_code=400,detail="No valid firms found")
    conn=get_db(); inserted=updated=0
    for f in firms:
        r=upsert_firm(conn,f,batch_id)
        if r=="inserted": inserted+=1
        else: updated+=1
    conn.execute("INSERT OR REPLACE INTO batches VALUES (?,?,?,?)",(batch_id,batch_name,len(firms),datetime.now().isoformat()))
    conn.commit(); conn.close()
    return {"batch_id":batch_id,"total":len(firms),"inserted":inserted,"updated":updated,"errors":len(errors)}

@app.post("/api/firm")
async def add_firm_manual(request: Request, payload: dict):
    check_auth(request)
    crd=payload.get("crd","").strip()
    if not crd: raise HTTPException(status_code=400,detail="CRD required")
    r={"crd":crd,"name":payload.get("name","").strip(),"city":payload.get("city","").strip(),"state":payload.get("state","").strip(),"classification":payload.get("classification","Independent RIA"),"aum_m":float(payload.get("aum_m",0)),"total_accounts":int(payload.get("total_accounts",0)),"advisor_count":max(1,int(payload.get("advisor_count",1))),"activities":payload.get("activities",""),"fee_structure":payload.get("fee_structure",""),"retail_custodian":payload.get("retail_custodian",""),"cagr1":float(payload["cagr1"]) if payload.get("cagr1") not in (None,"","—") else None,"cagr3":float(payload["cagr3"]) if payload.get("cagr3") not in (None,"","—") else None,"cagr5":float(payload["cagr5"]) if payload.get("cagr5") not in (None,"","—") else None,"firm_type":payload.get("firm_type","independent"),"fees":{"perf":bool(payload.get("fee_perf",False)),"comm":bool(payload.get("fee_comm",False)),"aum_only":bool(payload.get("fee_aum_only",True))},"pref_custodian":any(p in payload.get("retail_custodian","").lower() for p in ["schwab","fidelity","assetmark","national financial"]),"manual_entry":True}
    scored=score_firm(r); firm={**r,**scored}
    conn=get_db(); upsert_firm(conn,firm,"manual")
    conn.execute("INSERT OR IGNORE INTO batches VALUES (?,?,?,?)","manual","Manual entries",0,datetime.now().isoformat()); conn.execute("UPDATE batches SET firm_count=(SELECT COUNT(*) FROM firms WHERE batch_id='manual') WHERE id='manual'")
    conn.commit(); conn.close()
    return {"success":True,"composite":scored["composite"],"tier":scored["tier"],"geo_label":scored.get("geo_label","")}

@app.get("/api/firms")
async def get_firms(request: Request, tier: Optional[str]=None, services: Optional[str]=None, client_type: Optional[str]=None, firm_type: Optional[str]=None, geo_tier: Optional[int]=None, search: Optional[str]=None, batch_id: Optional[str]=None, limit: int=100, offset: int=0, sort_by: str="composite", sort_dir: str="desc", state: Optional[str]=None, city: Optional[str]=None, aum_min: Optional[float]=None, aum_max: Optional[float]=None):
    check_auth(request)
    conn=get_db(); where="WHERE 1=1"; params=[]
    if tier: where+=" AND tier=?"; params.append(tier)
    if services: where+=" AND services_label=?"; params.append(services)
    if client_type: where+=" AND client_tier=?"; params.append(client_type)
    if firm_type=="independent": where+=" AND firm_type='independent'"
    elif firm_type=="other": where+=" AND firm_type!='independent'"
    if geo_tier: where+=" AND geo_tier=?"; params.append(geo_tier)
    if search: where+=" AND (name LIKE ? OR crd LIKE ?)"; params.extend([f"%{search}%",f"%{search}%"])
    if batch_id: where+=" AND batch_id=?"; params.append(batch_id)
    if state: where+=" AND state=?"; params.append(state)
    if city: where+=" AND city=?"; params.append(city)
    if aum_min is not None: where+=" AND aum_m>=?"; params.append(aum_min)
    if aum_max is not None: where+=" AND aum_m<=?"; params.append(aum_max)
    total=conn.execute(f"SELECT COUNT(*) FROM firms {where}",params).fetchone()[0]
    sc=sort_by if sort_by in {"composite","name","aum_m","tier","scored_at"} else "composite"
    sd="ASC" if sort_dir.lower()=="asc" else "DESC"
    rows=conn.execute(f"SELECT * FROM firms {where} ORDER BY {sc} {sd} LIMIT ? OFFSET ?",[*params,limit,offset]).fetchall()
    conn.close()
    firms=[]
    for r in rows:
        f=dict(r); f["flags"]=json.loads(f["flags"] or "[]"); f["dims"]=json.loads(f["dims"] or "{}"); firms.append(f)
    return {"firms":firms,"total":total,"limit":limit,"offset":offset}

@app.get("/api/states")
async def get_states():
    conn=get_db()
    rows=conn.execute("SELECT DISTINCT state FROM firms WHERE state IS NOT NULL AND state != '' ORDER BY state").fetchall()
    conn.close()
    return {"states":[r["state"] for r in rows]}

@app.get("/api/cities")
async def get_cities(state: Optional[str]=None):
    conn=get_db()
    if state:
        rows=conn.execute("SELECT DISTINCT city FROM firms WHERE state=? AND city IS NOT NULL AND city != '' ORDER BY city",(state,)).fetchall()
    else:
        rows=conn.execute("SELECT DISTINCT city FROM firms WHERE city IS NOT NULL AND city != '' ORDER BY city").fetchall()
    conn.close()
    return {"cities":[r["city"] for r in rows]}

@app.get("/api/stats")
async def get_stats(request: Request):
    check_auth(request)
    conn=get_db()
    total=conn.execute("SELECT COUNT(*) FROM firms").fetchone()[0]
    tier_a=conn.execute("SELECT COUNT(*) FROM firms WHERE tier='A'").fetchone()[0]
    avg_row=conn.execute("SELECT AVG(composite) FROM firms").fetchone()[0]
    demoted=conn.execute("SELECT COUNT(*) FROM firms WHERE services_demoted=1").fetchone()[0]
    pushed=conn.execute("SELECT COUNT(*) FROM firms WHERE push_status='pushed'").fetchone()[0]
    batches=conn.execute("SELECT * FROM batches ORDER BY created_at DESC LIMIT 10").fetchall()
    conn.close()
    return {"total":total,"tier_a":tier_a,"avg_score":round(avg_row,1) if avg_row else 0,"demoted":demoted,"pushed":pushed,"batches":[dict(b) for b in batches]}

@app.post("/api/push/{crd}")
async def push_firm(request: Request, crd: str):
    check_auth(request)
    conn=get_db()
    firm=conn.execute("SELECT * FROM firms WHERE crd=?",(crd,)).fetchone()
    if not firm: raise HTTPException(status_code=404,detail="Firm not found")
    firm=dict(firm); firm["flags"]=json.loads(firm["flags"] or "[]")
    settings={r["key"]:r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
    api_key=settings.get("close_api_key","")
    if not api_key: raise HTTPException(status_code=400,detail="Close CRM API key not configured in Settings.")
    result=await push_to_close(firm,api_key)
    if result["success"]:
        conn.execute("UPDATE firms SET push_status='pushed', push_date=? WHERE crd=?",(datetime.now().isoformat(),crd)); conn.commit()
    conn.close(); return result

@app.post("/api/push-all")
async def push_all(request: Request, background_tasks: BackgroundTasks, tier: Optional[str]=None):
    check_auth(request)
    conn=get_db()
    settings={r["key"]:r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
    api_key=settings.get("close_api_key","")
    if not api_key: raise HTTPException(status_code=400,detail="Close CRM API key not configured.")
    q="SELECT * FROM firms WHERE push_status='pending'"; p=[]
    if tier: q+=" AND tier=?"; p.append(tier)
    firms=[dict(r) for r in conn.execute(q,p).fetchall()]
    conn.close()
    background_tasks.add_task(push_all_to_close,firms,api_key,DB_PATH)
    return {"queued":len(firms)}

@app.get("/api/settings")
async def get_settings(request: Request):
    check_auth(request)
    conn=get_db()
    s={r["key"]:r["value"] for r in conn.execute("SELECT * FROM settings").fetchall()}
    conn.close()
    if "close_api_key" in s:
        k=s["close_api_key"]; s["close_api_key"]=k[:8]+"..."+k[-4:] if len(k)>12 else "***"
    return s

@app.post("/api/settings")
async def save_settings(request: Request, payload: dict):
    check_auth(request)
    conn=get_db()
    for k,v in payload.items(): conn.execute("INSERT OR REPLACE INTO settings VALUES (?,?)",(k,v))
    conn.commit(); conn.close(); return {"saved":True}

@app.get("/api/export")
async def export_csv(request: Request, tier: Optional[str]=None, services: Optional[str]=None, batch_id: Optional[str]=None):
    check_auth(request)
    conn=get_db(); where="WHERE 1=1"; params=[]
    if tier: where+=" AND tier=?"; params.append(tier)
    if services: where+=" AND services_label=?"; params.append(services)
    if batch_id: where+=" AND batch_id=?"; params.append(batch_id)
    rows=[dict(r) for r in conn.execute(f"SELECT * FROM firms {where} ORDER BY composite DESC",params).fetchall()]
    conn.close()
    out=io.StringIO()
    out.write("CRD,Firm Name,City,State,AUM ($M),Total Accounts,Avg Account Size,Client Tier,Score,Tier,Services Fit,Geo Tier,Nearest Metro,Miles to Metro,Nearest SEIA,Miles to SEIA,Classification,Flags,Push Status,Batch,Scored At\n")
    for f in rows:
        flags=json.loads(f.get("flags") or "[]")
        flag_str="|".join([fl.get("t","") if isinstance(fl,dict) else str(fl) for fl in flags])
        out.write(f'"{f["crd"]}","{f["name"]}","{f["city"]}","{f["state"]}",{round(f["aum_m"] or 0,1)},{f["total_accounts"] or 0},"{f["avg_acct"] or ""}","{f["client_tier_label"] or ""}",{f["composite"]},{f["tier"]},"{f["services_label"]}",{f["geo_tier"]},"{f.get("nearest_metro","") or ""}",{f.get("dist_metro_miles") or ""},"{f.get("nearest_seia","") or ""}",{f.get("dist_seia_miles") or ""},"{f["classification"] or ""}","{flag_str}","{f["push_status"]}","{f["batch_id"]}","{f["scored_at"]}"\n')
    return Response(content=out.getvalue(),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=SEIA_RIA_Scores.csv"})

@app.delete("/api/firms")
async def clear_firms(request: Request, batch_id: Optional[str]=None):
    check_auth(request)
    conn=get_db()
    if batch_id:
        conn.execute("DELETE FROM firms WHERE batch_id=?",(batch_id,)); conn.execute("DELETE FROM batches WHERE id=?",(batch_id,))
    else:
        conn.execute("DELETE FROM firms"); conn.execute("DELETE FROM batches")
    conn.commit(); conn.close(); return {"cleared":True}
