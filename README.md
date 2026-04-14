# SEIA M&A Target Scorer

A local Python web application for scoring RIA acquisition targets using FINTRX data.

## Requirements

- Python 3.9 or higher
- A FINTRX CSV export

## Setup & Running

### Mac / Linux
```bash
chmod +x start.sh
./start.sh
```

### Windows
Double-click `start.bat` or run it from Command Prompt.

### Manual setup (any OS)
```bash
python3 -m venv venv
source venv/bin/activate          # Mac/Linux
# OR
venv\Scripts\activate             # Windows

pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Then open **http://localhost:8000** in your browser.

---

## How it works

1. **Upload** — Drop your FINTRX CSV export on the Upload tab. Firms are scored and stored in a local SQLite database (`seia_scorer.db`). Subsequent uploads add to the database; firms with matching CRDs are rescored and updated.

2. **Results** — Browse, filter, and search all scored firms. Paginated at 100 per page — handles 6,500+ firms without issue. Click any row to expand the full dimension breakdown.

3. **Push to Close** — Individual firms or bulk push by tier. Requires your Close API key configured in Settings, and the following custom fields on your Close lead records:
   - `cf_CRD` (text) — must match the Firm CRD from FINTRX
   - `cf_SEIA_MA_Score` (number)
   - `cf_SEIA_Tier` (text)
   - `cf_Services_Fit` (text)
   - `cf_Client_Tier` (text)
   - `cf_Avg_Account_Size` (text)
   - `cf_AUM_M` (number)

4. **Export** — Downloads a CSV of all scored firms (or filtered subset) for sharing or archiving.

---

## Scoring dimensions (v9)

| Dimension | Weight | Notes |
|---|---|---|
| Services alignment | 22% | Financial planning focus; pooled vehicle demotion caps at 45 |
| Client quality | 15% | Avg account size (UHNW→retail) + account count overlay |
| Geography | 13% | Tier 1 whitespace / Tier 2 SEIA present / Tier 3 secondary |
| AUM fit | 13% | Bell curve: $100M floor, $250M–$4B sweet spot |
| CAGR normalized | 11% | vs peer cohort, track record weighted |
| Succession pressure | 8% | Derived from advisory headcount |
| AUM per advisor | 7% | $75M floor |
| Growth consistency | 5% | 1/3/5yr volatility, negative year penalty |
| Fee structure | 4% | AUM-only best; commissions penalized |
| Custodian alignment | 2% | Schwab, Fidelity, AssetMark boost |

**Tier cutoffs:** A = 78+, B = 63–77, C = 48–62, D < 48

**SEIA offices (Tier 2):** Century City, Newport Beach, Tysons VA, Miami, Austin, San Francisco, Phoenix

---

## Files

```
seia_scorer/
├── main.py          # FastAPI application and API routes
├── scorer.py        # Scoring engine
├── close_crm.py     # Close CRM integration
├── requirements.txt # Python dependencies
├── start.sh         # Mac/Linux launcher
├── start.bat        # Windows launcher
├── templates/
│   └── index.html   # Frontend UI
└── seia_scorer.db   # SQLite database (created on first run)
```

---

## Notes

- The database file `seia_scorer.db` lives in the app directory and persists between sessions
- The app runs entirely locally — no data leaves your machine except when pushing to Close
- Tested with FINTRX standard CSV export format
- For large uploads (6,000+ firms), scoring takes 10–30 seconds depending on machine
