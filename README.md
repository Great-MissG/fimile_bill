
# Shipper Portal — Filter → Preview → Export (+API ready)

## Run (macOS)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py

## What it does
- Upload ONE CSV/XLSX
- Filter rows by Shipper Name (default: WYD China)
- Show only Tracking ID, Status, Address
- Preview table and export to Excel
- API enrichment UI is scaffolded (disabled by default). Enable later when you have the API.
