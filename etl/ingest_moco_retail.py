import os, sys, requests, duckdb
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv("MOCO_RETAIL_CSV_URL")
DB_PATH = os.getenv("DUCKDB_PATH", ".duckdb/moco_retail.duckdb")

if not URL:
    print("ERROR: MOCO_RETAIL_CSV_URL is missing"); sys.exit(1)

Path("data/raw").mkdir(parents=True, exist_ok=True)
Path(Path(DB_PATH).parent).mkdir(parents=True, exist_ok=True)

csv_path = Path("data/raw/warehouse_and_retail_sales.csv")
print(f"Downloading -> {csv_path}")
with requests.get(URL, stream=True, timeout=120) as r:
    r.raise_for_status()
    with open(csv_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1<<20):
            if chunk: f.write(chunk)

print(f"Loading into DuckDB -> {DB_PATH}")
con = duckdb.connect(DB_PATH)
con.execute("""
    CREATE OR REPLACE TABLE raw_moco_retail AS
    SELECT * FROM read_csv_auto(?, HEADER=TRUE, SAMPLE_SIZE=-1);
""", [str(csv_path)])

rows = con.execute("SELECT COUNT(*) FROM raw_moco_retail").fetchone()[0]
print(f"Ingested rows: {rows}")
print("Done.")
con.close()
