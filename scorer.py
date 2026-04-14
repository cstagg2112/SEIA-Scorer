import math
import zipcodes
from typing import Optional
from functools import lru_cache

MAJOR_METROS = {
    "New York":(40.7128,-74.0060),"Los Angeles":(34.0522,-118.2437),"Chicago":(41.8781,-87.6298),
    "Dallas":(32.7767,-96.7970),"Houston":(29.7604,-95.3698),"Washington DC":(38.9072,-77.0369),
    "Miami":(25.7617,-80.1918),"Philadelphia":(39.9526,-75.1652),"Atlanta":(33.7490,-84.3880),
    "Boston":(42.3601,-71.0589),"San Francisco":(37.7749,-122.4194),"Seattle":(47.6062,-122.3321),
    "Denver":(39.7392,-104.9903),"Minneapolis":(44.9778,-93.2650),"San Diego":(32.7157,-117.1611),
    "Tampa":(27.9506,-82.4572),"Portland":(45.5051,-122.6750),"Charlotte":(35.2271,-80.8431),
    "Nashville":(36.1627,-86.7816),"Austin":(30.2672,-97.7431),"Salt Lake City":(40.7608,-111.8910),
    "Kansas City":(39.0997,-94.5786),"Indianapolis":(39.7684,-86.1581),"Las Vegas":(36.1699,-115.1398),
    "Detroit":(42.3314,-83.0458),"San Antonio":(29.4241,-98.4936),"Raleigh":(35.7796,-78.6382),
    "Pittsburgh":(40.4406,-79.9959),"Baltimore":(39.2904,-76.6122),"Hartford":(41.7658,-72.6851),
}
SEIA_OFFICES = {
    "Century City":(34.0560,-118.4154),"Newport Beach":(33.6189,-117.9289),
    "Tysons":(38.9187,-77.2311),"Miami":(25.7617,-80.1918),
    "Austin":(30.2672,-97.7431),"San Francisco":(37.7749,-122.4194),"Phoenix":(33.4484,-112.0740),
}
PREF_CUSTODIANS=["schwab","charles schwab","fidelity","assetmark","national financial"]
WEIGHTS={"services":22,"client_qual":15,"geo":13,"aum":13,"cagr":11,"succession":8,"aum_adv":7,"consistency":5,"fee":4,"custodian":2}

def haversine(lat1,lon1,lat2,lon2):
    R=3958.8
    phi1,phi2=math.radians(lat1),math.radians(lat2)
    dphi=math.radians(lat2-lat1);dlambda=math.radians(lon2-lon1)
    a=math.sin(dphi/2)**2+math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

@lru_cache(maxsize=4096)
def city_state_to_coords(city,state):
    cc=city.strip().title();sc=state.strip().upper()
    m=zipcodes.filter_by(city=cc,state=sc)
    if m:return float(m[0]["lat"]),float(m[0]["long"])
    all_s=zipcodes.filter_by(state=sc);cl=city.lower().strip()
    for z in all_s:
        if cl==z["city"].lower():return float(z["lat"]),float(z["long"])
        if cl in [c.lower() for c in z.get("acceptable_cities",[])]:return float(z["lat"]),float(z["long"])
    for z in all_s:
        if cl in z["city"].lower() and len(cl)>3:return float(z["lat"]),float(z["long"])
    return None

def score_geography(city,state):
    coords=city_state_to_coords(city,state)
    if not coords:return{"score":50,"geo_tier":3,"nearest_metro":"Unknown","nearest_seia":None,"dist_metro_miles":None,"dist_seia_miles":None,"geo_label":"Unknown (could not geocode)"}
    lat,lon=coords
    md={n:haversine(lat,lon,mlat,mlon) for n,(mlat,mlon) in MAJOR_METROS.items()}
    nm=min(md,key=md.get);dm=md[nm]
    sd={n:haversine(lat,lon,slat,slon) for n,(slat,slon) in SEIA_OFFICES.items()}
    ns=min(sd,key=sd.get);ds=sd[ns]
    if dm<=15:ms=95
    elif dm<=30:ms=90
    elif dm<=50:ms=82
    elif dm<=75:ms=72
    elif dm<=100:ms=60
    elif dm<=150:ms=45
    else:ms=25
    if ds<=50:
        fs=min(ms,62);gt=2;gl=f"SEIA present ({ns}, {round(ds)}mi) · {nm} {round(dm)}mi"
    else:
        fs=ms;gt=1 if ms>=60 else 3;gl=f"Whitespace · {nm} {round(dm)}mi"
    return{"score":fs,"geo_tier":gt,"nearest_metro":nm,"nearest_seia":ns,"dist_metro_miles":round(dm,1),"dist_seia_miles":round(ds,1),"geo_label":gl}

def clean_str(val):
    if val is None or(isinstance(val,float) and str(val)=="nan"):return ""
    return str(val).strip()
def parse_aum(val):
    s=clean_str(val).replace("$","").replace(",","").replace(" ","")
    try:return float(s)/1_000_000
    except:return 0.0
def parse_pct(val):
    s=clean_str(val).replace("%","").strip()
    if not s or s in("N/A","n/a","-"):return None
    try:return float(s)
    except:return None
def parse_int(val):
    s=clean_str(val).replace(",","")
    try:return int(float(s))
    except:return 0
def detect_custodian(c):return any(p in c.lower() for p in PREF_CUSTODIANS)
def detect_fees(f):
    s=f.lower()
    return{"perf":"performance based" in s,"comm":"commission" in s,"aum_only":"percentage of aum" in s and "performance" not in s and "commission" not in s}
def detect_firm_type(c):
    s=c.lower()
    if "independent ria" in s:return "independent"
    if any(x in s for x in["private equity","hedge fund","real estate fund","securitized"]):return "fund"
    if any(x in s for x in["insurance","broker dealer"]):return "bd"
    return "other"
def score_services(a):
    s=a.lower()
    if "pooled investment vehicles" in s or "investment companies" in s:return{"score":20,"label":"demoted","demoted":True}
    v=0
    if "financial planning" in s:v+=35
    if "individuals/small businesses" in s:v+=25
    if "pension consulting" in s:v+=15
    if "selection of other advisers" in s:v+=10
    if "educational seminars" in s:v+=5
    if "businesses or institutions" in s and "individuals/small businesses" not in s:v=min(v,35)
    v=min(100,v);return{"score":v,"label":"high" if v>=75 else "mid" if v>=45 else "low","demoted":False}
def score_client_quality(aum_m,total_accounts):
    if not total_accounts or total_accounts<=0:return{"score":50,"tier":"unknown","tier_label":"—","avg_acct":None}
    avg_m=aum_m/total_accounts;avg_k=avg_m*1000
    if avg_m>=5:base,tier,lbl=100,"uhnw","UHNW"
    elif avg_m>=1:base,tier,lbl=85,"hnw","HNW"
    elif avg_k>=500:base,tier,lbl=60,"uma","Upper mass affluent"
    elif avg_k>=250:base,tier,lbl=35,"mass","Mass affluent"
    else:base,tier,lbl=15,"retail","Retail"
    mod=0
    if total_accounts<100:mod+=5
    if total_accounts>10000:mod-=10
    elif total_accounts>5000:mod-=8
    score=min(100,max(0,base+mod))
    return{"score":score,"tier":tier,"tier_label":lbl,"avg_acct":f"${avg_m:.1f}M" if avg_m>=1 else f"${round(avg_k)}K"}
def score_aum(m):
    if m<100:return max(0,round((m/100)*30))
    if m<250:return round(30+((m-100)/150)*52)
    if m<=4000:return round(82+((m-250)/3750)*15)
    return max(35,round(97-((m-4000)/2000)*35))
def score_consistency(c1,c3,c5):
    vals=[v for v in[c1,c3/3 if c3 else None,c5/5 if c5 else None] if v is not None]
    if len(vals)<2:return{"score":50,"neg_years":0,"std":0}
    mean=sum(vals)/len(vals);std=math.sqrt(sum((v-mean)**2 for v in vals)/len(vals))
    neg=sum(1 for v in vals if v<0)
    return{"score":min(100,max(0,round(100-std*4-neg*15))),"neg_years":neg,"std":round(std,1)}
def parse_fintrx_row(row):
    def col(*names):
        for name in names:
            for cn in row.index:
                if name.lower() in cn.lower():return clean_str(row[cn])
        return ""
    emp=parse_int(col("employees"));ins=parse_int(col("insurance agents"));bd=parse_int(col("broker dealer reps","broker dealer"))
    city=col("main office city");state=col("main office state");activities=col("advisory activities")
    fee_str=col("fee structure");custodian=col("retail custodian")
    aum_m=parse_aum(col("total aum"));total_accounts=parse_int(col("total accounts"))
    classification=col("firm classification","classification")
    return{"crd":col("firm crd"),"name":col("firm name"),"city":city,"state":state,"classification":classification,
           "aum_m":aum_m,"total_accounts":total_accounts,"advisor_count":max(1,emp-ins-bd),
           "activities":activities,"fee_structure":fee_str,"retail_custodian":custodian,
           "cagr1":parse_pct(col("yoy aum change")),"cagr3":parse_pct(col("3 year aum change")),"cagr5":parse_pct(col("5 year aum change")),
           "firm_type":detect_firm_type(classification),"fees":detect_fees(fee_str),"pref_custodian":detect_custodian(custodian)}
def score_firm(r):
    dims={};aum_m=r["aum_m"]
    svc=score_services(r["activities"]);cq=score_client_quality(aum_m,r["total_accounts"]);geo=score_geography(r["city"],r["state"])
    dims["services"]=svc["score"];dims["client_qual"]=cq["score"];dims["geo"]=geo["score"];dims["aum"]=score_aum(aum_m)
    c1,c3,c5=r.get("cagr1"),r.get("cagr3"),r.get("cagr5")
    cr=c5/5 if c5 else(c3/3 if c3 else c1);ty=5 if c5 else(3 if c3 else 1)
    bench=5 if aum_m>2000 else 6 if aum_m>1000 else 8 if aum_m>500 else 10
    tm=1.1 if ty>=5 else 1.0 if ty>=3 else 0.85
    dims["cagr"]=min(100,max(0,round(min(100,max(0,50+((cr-bench)/bench)*50))*tm))) if cr is not None else 50
    cons=score_consistency(c1,c3,c5);dims["consistency"]=cons["score"]
    adv=r["advisor_count"];dims["succession"]=75 if adv<=2 else 55 if adv<=5 else 40 if adv<=10 else 30
    apa=aum_m/adv if adv>0 else 0
    dims["aum_adv"]=90 if apa>=150 else min(100,round(60+((apa-75)/75)*30)) if apa>=75 else min(100,round((apa/75)*50))
    fees=r["fees"];dims["fee"]=92 if fees["aum_only"] else 30 if fees["comm"] else 35 if fees["perf"] else 60
    dims["custodian"]=88 if r["pref_custodian"] else 55
    tw=sum(WEIGHTS.values());composite=sum(dims.get(k,50)*(v/tw) for k,v in WEIGHTS.items())
    if svc["demoted"]:composite=min(composite,45)
    composite=round(composite);tier="A" if composite>=78 else "B" if composite>=63 else "C" if composite>=48 else "D"
    flags=[]
    if svc["demoted"]:flags.append({"t":"Pooled/fund model","dem":True})
    if r["firm_type"]=="fund":flags.append({"t":"PE/Hedge/RE","warn":True})
    if r["firm_type"]=="bd":flags.append({"t":"Ins/BD","warn":True})
    if fees["perf"]:flags.append({"t":"Perf fees","warn":True})
    if fees["comm"]:flags.append({"t":"Commissions","warn":False})
    if c1 is not None and c1<0:flags.append({"t":"Neg YoY","warn":True})
    if cq["tier"]=="retail":flags.append({"t":"Retail accts","warn":False})
    return{"composite":composite,"tier":tier,"services_score":svc["score"],"services_label":svc["label"],"services_demoted":svc["demoted"],
           "client_qual_score":cq["score"],"client_tier":cq["tier"],"client_tier_label":cq["tier_label"],"avg_acct":cq["avg_acct"],
           "geo_tier":geo["geo_tier"],"nearest_metro":geo["nearest_metro"],"dist_metro_miles":geo["dist_metro_miles"],
           "nearest_seia":geo["nearest_seia"],"dist_seia_miles":geo["dist_seia_miles"],"geo_label":geo["geo_label"],
           "flags":flags,"dims":{k:dims.get(k,50) for k in WEIGHTS}}
