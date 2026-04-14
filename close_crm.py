import httpx
import sqlite3
import json
from datetime import datetime
from typing import Optional


CLOSE_API_BASE = "https://api.close.com/api/v1"


async def find_lead_by_crd(crd: str, api_key: str) -> Optional[str]:
    """Find a Close lead by CRD custom field value."""
    async with httpx.AsyncClient(auth=(api_key, ""), timeout=15) as client:
        resp = await client.get(
            f"{CLOSE_API_BASE}/lead/",
            params={"query": f'custom.cf_CRD:"{crd}"', "_fields": "id,display_name"}
        )
        if resp.status_code == 200:
            data = resp.json()
            leads = data.get("data", [])
            if leads:
                return leads[0]["id"]
    return None


async def update_lead_custom_fields(lead_id: str, firm: dict, api_key: str) -> dict:
    """Push score, tier, and client quality to Close custom fields."""
    payload = {
        "custom": {
            "cf_SEIA_MA_Score": firm["composite"],
            "cf_SEIA_Tier": firm["tier"],
            "cf_Services_Fit": firm.get("services_label", ""),
            "cf_Client_Tier": firm.get("client_tier_label", ""),
            "cf_Avg_Account_Size": firm.get("avg_acct", ""),
            "cf_AUM_M": round(firm.get("aum_m", 0), 1),
        }
    }
    async with httpx.AsyncClient(auth=(api_key, ""), timeout=15) as client:
        resp = await client.put(f"{CLOSE_API_BASE}/lead/{lead_id}/", json=payload)
        return {"status_code": resp.status_code, "ok": resp.status_code == 200}


async def push_to_close(firm: dict, api_key: str) -> dict:
    """Find lead by CRD and push scoring data. Returns result dict."""
    crd = firm.get("crd", "")
    if not crd:
        return {"success": False, "error": "No CRD number on firm record"}

    try:
        lead_id = await find_lead_by_crd(crd, api_key)
        if not lead_id:
            return {
                "success": False,
                "error": f"No Close lead found with CRD {crd}. Ensure the CRD custom field exists on the lead."
            }

        result = await update_lead_custom_fields(lead_id, firm, api_key)
        if result["ok"]:
            return {"success": True, "lead_id": lead_id, "crd": crd}
        else:
            return {"success": False, "error": f"Close API returned {result['status_code']}"}

    except httpx.TimeoutException:
        return {"success": False, "error": "Close API request timed out"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def push_all_to_close(firms: list, api_key: str, db_path: str):
    """Background task: push all pending firms to Close, update DB as each completes."""
    conn = sqlite3.connect(db_path)
    for firm in firms:
        firm["flags"] = json.loads(firm.get("flags") or "[]")
        firm["dims"] = json.loads(firm.get("dims") or "{}")
        result = await push_to_close(firm, api_key)
        if result["success"]:
            conn.execute(
                "UPDATE firms SET push_status='pushed', push_date=? WHERE crd=?",
                (datetime.now().isoformat(), firm["crd"])
            )
            conn.commit()
    conn.close()
