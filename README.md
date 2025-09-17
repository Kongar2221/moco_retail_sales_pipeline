Project: Pipline_Resale (Tableau)

Live dashboard:
https://public.tableau.com/views/Pipline_Resale/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link


WHAT THIS IS
------------
One dashboard that shows resale + returns over time. Monthly data. You can filter by date, supplier, and item type. It’s meant for people who want to poke around the numbers fast and actually see what’s going on.


WHAT YOU CAN SEE
----------------
- Total Sales (by month)
- Returns (6-month view) and Returns %
- Warehouse Mix % (how much of sales came from warehouse channel)
- Top Supplier (latest month)
- Top Item Type (recent window + share)
- YoY Growth (latest month vs last year)

There are detail charts for each KPI:
- Sales Decomposition (stacked bars: Retail, Transfers, Warehouse)
- Supplier Trend (many suppliers over time, with share on a second axis)
- Returns Trend ($ and % of total)
- Item Type Over Time (lines, latest labels)


HOW TO USE
----------
- Date filter: slide the range to focus on the months you care about.
- Supplier filter: pick one or many suppliers.
- Item Type filter: pick a category.
- Everything updates together. Hover for tooltips. Click to highlight where supported.

Note: if you want filters (like Date) to also change FIXED/LOD KPI calcs, open any worksheet in Tableau and make the Date filter a “Context” filter. If you don’t do that, LOD KPIs stay anchored (which is fine for most cases).


DATA (short version)
--------------------
- fact_monthly_sales.csv
  - Sale Date (month)
  - Measures: Retail Sales, Retail Transfers, Warehouse Sales, Total Sales
  - Supplier, Item Code, Item Description
- dim_item.csv
  - Item Code, Item Description, Item Type, Supplier

Monthly grain. Total Sales = Retail + Transfers + Warehouse. Extra calc fields used for latest month flags, YoY, rolling windows, etc.


HOW IT WAS BUILT (simple notes)
-------------------------------
- KPI tiles first, then the trend charts.
- LOD calcs for “last full month”, 6-month windows, prior year, etc.
- Table calcs for rolling averages and up/down coloring.
- Consistent formats: currency in K, percentages with 1 decimal.
- Minimal gridlines, clear axis titles, and one legend per row.


REPO LAYOUT (example)
---------------------
/data
  dim_item.csv
  fact_monthly_sales.csv

/workbook
  Pipline_Resale.twb

README.txt  (this file)


OPEN LOCALLY
------------
1) Clone the repo.
2) Open /workbook/Pipline_Resale.twb in Tableau Desktop.
3) If Tableau asks, point the connections to the files in /data.
4) Refresh. Check filters and number formats.


LIVE VIEW (recommended)
-----------------------
Open the public link and interact there:
https://public.tableau.com/views/Pipline_Resale/Dashboard1?:language=en-US&:display_count=n&:origin=viz_share_link