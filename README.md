# Keyword Brands Pipeline

## Quick start
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export KEYWORDTOOL_KEY=...
export KEYAPP_KEY=...
export APPSTORESPY_KEY=...

python3 run_pipeline.py --country br --caps 500000 --sort desc --sort-by volume
