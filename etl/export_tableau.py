from pathlib import Path
import os, duckdb
from dotenv import load_dotenv

load_dotenv()

db_path = os.getenv("DUCKDB_PATH", ".duckdb/moco_retail.duckdb")
out_dir = Path("exports/tableau")
out_dir.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(db_path)

# build POSIX-safe, quoted paths for COPY
fact_path = (out_dir / "fact_monthly_sales.csv").as_posix().replace("'", "''")
dim_path  = (out_dir / "dim_item.csv").as_posix().replace("'", "''")

# 1) flattened monthly fact with item description
con.execute(f"""
COPY (
  SELECT
    f.sale_date,
    f.item_code,
    COALESCE(d.item_description, '') AS item_description,
    f.supplier,
    f.retail_sales,
    f.retail_transfers,
    f.warehouse_sales,
    (f.retail_sales + f.retail_transfers + f.warehouse_sales) AS total_sales
  FROM main.fct_monthly_sales f
  LEFT JOIN main.dim_item d USING (item_code)
  ORDER BY f.sale_date, f.item_code
) TO '{fact_path}'
WITH (HEADER, DELIMITER ',', QUOTE '"', DATEFORMAT '%Y-%m-%d');
""")

# 2) optional dimension export
con.execute(f"""
COPY (
  SELECT * FROM main.dim_item ORDER BY item_code
) TO '{dim_path}'
WITH (HEADER, DELIMITER ',', QUOTE '"');
""")

rows = con.execute("SELECT COUNT(*) FROM main.fct_monthly_sales").fetchone()[0]
print(f"exported {rows} rows -> {out_dir/'fact_monthly_sales.csv'}")

con.close()
