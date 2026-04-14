import re
from typing import Optional

TIER2_MARKETS = [
    "century city", "los angeles", " la ", "west los angeles",
    "newport beach", "orange county", "irvine", "costa mesa",
    "tysons", "mclean", "northern virginia", "nova",
    "miami", "miami beach", "coral gables", "south florida",
    "austin", "san francisco", "bay area",
    "phoenix", "scottsdale", "tempe",
]
TIER2_STATES = ["virginia", "texas", "california", "arizona", "florida"]

TIER1_MARKETS = [
    "dallas", "fort worth", "dfw", "houston",
    "seattle", "bellevue", "tacoma",
    "boston", "cambridge",
    "new york", "nyc", "manhattan", "brooklyn",
    "chicago", "denver", "boulder",
    "minneapolis", "atlanta", "portland",
    "san diego", "san antonio", "nashville",
    "charlotte", "tampa", "orlando",
    "las vegas", "detroit", "kansas city",
    "salt lake city", "indianapolis",
    "pittsburgh", "philadelphia", "raleigh", "durham",
    "baltimore", "washington dc", "washington",
]
TIER1_STATES = [
    "massachusetts", "new york", "new jersey", "illinois",
    "colorado", "minnesota", "georgia", "oregon",
    "tennessee", "north carolina", "nevada", "michigan",
    "missouri", "utah", "indiana", "pennsylvania", "maryland",
    "washington",
]

PREF_CUSTODIANS = ["schwab", "charles schwab", "fidelity", "assetmark", "national financial"]

WEIGHTS = {
    "services":    22,
    "client_qual": 15,
    "geo":         13,
    "aum":         13,
    "cagr":        11,
    "succession":  8,
    "aum_adv":     7,
    "consistency": 5,
    "fee":         4,
    "custodian":   2,
}


def clean_str(val) -> str:
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return ""
    return str(val).strip()


def parse_aum(val) -> float:
    s = clean_str(val).replace("$", "").replace(",", "").replace(" ", "")
    try:
        return float(s) / 1_000_000
    except ValueError:
        return 0.0


def parse_pct(val) -> Optional[float]:
    s = clean_str(val).replace("%", "").strip()
    if not s or s in ("N/A", "n/a", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_int(val) -> int:
    s = clean_str(val).replace(",", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def detect_geo_tier(city: str, state: str) -> int:
    loc = f"{city} {state}".lower()
    for m in TIER2_MARKETS:
        if m in loc:
            return 2
    for s in TIER2_STATES:
        if s in loc:
            return 2
    for m in TIER1_MARKETS:
        if m in loc:
            return 1
    for s in TIER1_STATES:
        if s in loc:
            return 1
    return 3


def detect_custodian(custodian: str) -> bool:
    c = custodian.lower()
    return any(p in c for p in PREF_CUSTODIANS)


def detect_fees(fee_str: str) -> dict:
    s = fee_str.lower()
    return {
        "perf": "performance based" in s,
        "comm": "commission" in s,
        "aum_only": "percentage of aum" in s and "performance" not in s and "commission" not in s,
    }


def detect_firm_type(classification: str) -> str:
    s = classification.lower()
    if "independent ria" in s:
        return "independent"
    if any(x in s for x in ["private equity", "hedge fund", "real estate fund", "securitized"]):
        return "fund"
    if any(x in s for x in ["insurance", "broker dealer"]):
        return "bd"
    return "other"


def score_services(activities: str) -> dict:
    a = activities.lower()
    if "pooled investment vehicles" in a or "investment companies" in a:
        return {"score": 20, "label": "demoted", "demoted": True}
    score = 0
    if "financial planning" in a:
        score += 35
    if "individuals/small businesses" in a:
        score += 25
    if "pension consulting" in a:
        score += 15
    if "selection of other advisers" in a:
        score += 10
    if "educational seminars" in a:
        score += 5
    if "businesses or institutions" in a and "individuals/small businesses" not in a:
        score = min(score, 35)
    score = min(100, score)
    label = "high" if score >= 75 else "mid" if score >= 45 else "low"
    return {"score": score, "label": label, "demoted": False}


def score_client_quality(aum_m: float, total_accounts: int) -> dict:
    if not total_accounts or total_accounts <= 0:
        return {"score": 50, "tier": "unknown", "tier_label": "—", "avg_acct": None}
    avg_m = aum_m / total_accounts
    avg_k = avg_m * 1000
    if avg_m >= 5:
        base, tier, tier_label = 100, "uhnw", "UHNW"
    elif avg_m >= 1:
        base, tier, tier_label = 85, "hnw", "HNW"
    elif avg_k >= 500:
        base, tier, tier_label = 60, "uma", "Upper mass affluent"
    elif avg_k >= 250:
        base, tier, tier_label = 35, "mass", "Mass affluent"
    else:
        base, tier, tier_label = 15, "retail", "Retail"
    mod = 0
    if total_accounts < 100:
        mod += 5
    if total_accounts > 10000:
        mod -= 10
    elif total_accounts > 5000:
        mod -= 8
    score = min(100, max(0, base + mod))
    if avg_m >= 1:
        avg_display = f"${avg_m:.1f}M"
    else:
        avg_display = f"${round(avg_k)}K"
    return {"score": score, "tier": tier, "tier_label": tier_label, "avg_acct": avg_display}


def score_aum(m: float) -> int:
    if m < 100:
        return max(0, round((m / 100) * 30))
    if m < 250:
        return round(30 + ((m - 100) / 150) * 52)
    if m <= 4000:
        return round(82 + ((m - 250) / 3750) * 15)
    return max(35, round(97 - ((m - 4000) / 2000) * 35))


def score_consistency(cagr1, cagr3, cagr5) -> dict:
    vals = []
    if cagr1 is not None:
        vals.append(cagr1)
    if cagr3 is not None:
        vals.append(cagr3 / 3)
    if cagr5 is not None:
        vals.append(cagr5 / 5)
    if len(vals) < 2:
        return {"score": 50, "neg_years": 0, "std": 0}
    mean = sum(vals) / len(vals)
    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = variance ** 0.5
    neg = sum(1 for v in vals if v < 0)
    score = min(100, max(0, round(100 - std * 4 - neg * 15)))
    return {"score": score, "neg_years": neg, "std": round(std, 1)}


def parse_fintrx_row(row) -> dict:
    def col(*names):
        for name in names:
            for col_name in row.index:
                if name.lower() in col_name.lower():
                    return clean_str(row[col_name])
        return ""

    emp = parse_int(col("employees"))
    ins = parse_int(col("insurance agents"))
    bd = parse_int(col("broker dealer reps", "broker dealer"))
    advisor_count = max(1, emp - ins - bd)

    city = col("main office city")
    state = col("main office state")
    activities = col("advisory activities")
    fee_str = col("fee structure")
    custodian = col("retail custodian")
    aum_m = parse_aum(col("total aum"))
    total_accounts = parse_int(col("total accounts"))
    classification = col("firm classification", "classification")

    return {
        "crd": col("firm crd"),
        "name": col("firm name"),
        "city": city,
        "state": state,
        "classification": classification,
        "aum_m": aum_m,
        "total_accounts": total_accounts,
        "advisor_count": advisor_count,
        "activities": activities,
        "fee_structure": fee_str,
        "retail_custodian": custodian,
        "cagr1": parse_pct(col("yoy aum change")),
        "cagr3": parse_pct(col("3 year aum change")),
        "cagr5": parse_pct(col("5 year aum change")),
        "geo_tier": detect_geo_tier(city, state),
        "firm_type": detect_firm_type(classification),
        "fees": detect_fees(fee_str),
        "pref_custodian": detect_custodian(custodian),
    }


def score_firm(r: dict) -> dict:
    dims = {}
    aum_m = r["aum_m"]
    svc = score_services(r["activities"])
    cq = score_client_quality(aum_m, r["total_accounts"])

    dims["services"] = svc["score"]
    dims["client_qual"] = cq["score"]
    dims["aum"] = score_aum(aum_m)

    cagr1, cagr3, cagr5 = r["cagr1"], r["cagr3"], r["cagr5"]
    cagr_raw = cagr5 / 5 if cagr5 is not None else (cagr3 / 3 if cagr3 is not None else cagr1)
    track_yrs = 5 if cagr5 is not None else (3 if cagr3 is not None else 1)
    bench = 5 if aum_m > 2000 else 6 if aum_m > 1000 else 8 if aum_m > 500 else 10
    t_mult = 1.1 if track_yrs >= 5 else 1.0 if track_yrs >= 3 else 0.85
    if cagr_raw is not None:
        raw_score = 50 + ((cagr_raw - bench) / bench) * 50
        dims["cagr"] = min(100, max(0, round(min(100, max(0, raw_score)) * t_mult)))
    else:
        dims["cagr"] = 50

    cons = score_consistency(cagr1, cagr3, cagr5)
    dims["consistency"] = cons["score"]

    adv = r["advisor_count"]
    dims["succession"] = 75 if adv <= 2 else 55 if adv <= 5 else 40 if adv <= 10 else 30

    apa = aum_m / adv if adv > 0 else 0
    if apa >= 150:
        dims["aum_adv"] = 90
    elif apa >= 75:
        dims["aum_adv"] = min(100, round(60 + ((apa - 75) / 75) * 30))
    else:
        dims["aum_adv"] = min(100, round((apa / 75) * 50))

    dims["geo"] = 95 if r["geo_tier"] == 1 else 62 if r["geo_tier"] == 2 else 22
    fees = r["fees"]
    dims["fee"] = 92 if fees["aum_only"] else 30 if fees["comm"] else 35 if fees["perf"] else 60
    dims["custodian"] = 88 if r["pref_custodian"] else 55

    tw = sum(WEIGHTS.values())
    composite = sum(dims.get(k, 50) * (v / tw) for k, v in WEIGHTS.items())
    if svc["demoted"]:
        composite = min(composite, 45)

    composite = round(composite)
    tier = "A" if composite >= 78 else "B" if composite >= 63 else "C" if composite >= 48 else "D"

    flags = []
    if svc["demoted"]:
        flags.append({"t": "Pooled/fund model", "dem": True})
    if r["firm_type"] == "fund":
        flags.append({"t": "PE/Hedge/RE", "warn": True})
    if r["firm_type"] == "bd":
        flags.append({"t": "Ins/BD", "warn": True})
    if fees["perf"]:
        flags.append({"t": "Perf fees", "warn": True})
    if fees["comm"]:
        flags.append({"t": "Commissions", "warn": False})
    if cagr1 is not None and cagr1 < 0:
        flags.append({"t": "Neg YoY", "warn": True})
    if cq["tier"] == "retail":
        flags.append({"t": "Retail accts", "warn": False})

    return {
        "composite": composite,
        "tier": tier,
        "services_score": svc["score"],
        "services_label": svc["label"],
        "services_demoted": svc["demoted"],
        "client_qual_score": cq["score"],
        "client_tier": cq["tier"],
        "client_tier_label": cq["tier_label"],
        "avg_acct": cq["avg_acct"],
        "flags": flags,
        "dims": {k: dims.get(k, 50) for k in WEIGHTS},
    }
