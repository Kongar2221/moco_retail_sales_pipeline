from pathlib import Path
import os, sys, json, hashlib, requests, duckdb
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("MOCO_RETAIL_CSV_URL")
db_path = os.getenv("DUCKDB_PATH", ".duckdb/moco_retail.duckdb")
if not url:
    sys.exit("missing MOCO_RETAIL_CSV_URL")

raw_dir = Path("data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)
state_file = raw_dir / "source_state.json"
csv_path = raw_dir / "warehouse_and_retail_sales.csv"

def load_state():
    try:
        return json.loads(state_file.read_text())
    except FileNotFoundError:
        return {}

def save_state(s: dict):
    state_file.write_text(json.dumps(s, indent=2))

def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

state = load_state()
headers = {}
if state.get("etag"):
    headers["if-none-match"] = state["etag"]
if state.get("last_modified"):
    headers["if-modified-since"] = state["last_modified"]

r = requests.get(url, stream=True, timeout=180, headers=headers)
if r.status_code != 304:
    r.raise_for_status()
    with csv_path.open("wb") as f:
        for chunk in r.iter_content(1 << 20):
            if chunk:
                f.write(chunk)
    state["etag"] = r.headers.get("etag")
    state["last_modified"] = r.headers.get("last-modified")
    state["sha256"] = sha256(csv_path)
    save_state(state)

con = duckdb.connect(db_path)

con.execute("""
  create table if not exists raw_moco_retail as
  select * from read_csv_auto('data/raw/warehouse_and_retail_sales.csv', header=true, sample_size=-1)
  limit 0
""")

con.execute("begin")

con.execute("""
  create or replace temporary table _new as
  select
    "YEAR"::bigint as year,
    "MONTH"::bigint as month,
    "SUPPLIER"::varchar as supplier,
    "ITEM CODE"::varchar as item_code,
    "ITEM DESCRIPTION"::varchar as item_description,
    "ITEM TYPE"::varchar as item_type,
    "RETAIL SALES"::double as retail_sales,
    "RETAIL TRANSFERS"::double as retail_transfers,
    "WAREHOUSE SALES"::double as warehouse_sales
  from read_csv_auto(?, header=true, sample_size=-1)
""", [str(csv_path)])

con.execute("""
  delete from raw_moco_retail t
  where exists (
    select 1
    from _new s
    where t."YEAR" = s.year
      and t."MONTH" = s.month
      and t."SUPPLIER" = s.supplier
      and t."ITEM CODE" = s.item_code
  )
""")

con.execute("""
  insert into raw_moco_retail
    ("YEAR","MONTH","SUPPLIER","ITEM CODE","ITEM DESCRIPTION","ITEM TYPE","RETAIL SALES","RETAIL TRANSFERS","WAREHOUSE SALES")
  select
    year, month, supplier, item_code, item_description, item_type,
    retail_sales, retail_transfers, warehouse_sales
  from _new
""")

con.execute("commit")

rows = con.execute("select count(*) from raw_moco_retail").fetchone()[0]
print(f"raw_moco_retail rowcount: {rows}")
con.close()
