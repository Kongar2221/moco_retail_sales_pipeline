import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
db_path = os.getenv("DUCKDB_PATH", ".duckdb/moco_retail.duckdb")
out_dir = Path(__file__).parent
out_file = out_dir / "raw_schema.txt"
con = duckdb.connect(db_path)
df = con.execute("PRAGMA table_info('raw_moco_retail');").fetchdf()
con.close()

out_dir.mkdir(parents=True, exist_ok=True)
with open(out_file, "w", encoding="utf-8") as f:
  f.write(df.to_string(index=False))

print(f"Wrote {len(df)} rows to {out_file}")