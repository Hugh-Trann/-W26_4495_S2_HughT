from src.db import get_conn

SQL_REBUILD_DAILY = """
DELETE FROM analytics.agg_sales_daily;

INSERT INTO analytics.agg_sales_daily (SalesDate, ProductID, Orders, Units, Revenue, AvgOrderValue)
SELECT
    CAST(PurchaseDate AS date) AS SalesDate,
    ProductID,
    COUNT(*) AS Orders,
    SUM(PurchaseQuantity) AS Units,
    CAST(SUM(PurchaseQuantity * PurchasePrice) AS decimal(18,2)) AS Revenue,
    CAST(SUM(PurchaseQuantity * PurchasePrice) / NULLIF(COUNT(*), 0) AS decimal(18,2)) AS AvgOrderValue
FROM clean.fact_sales
GROUP BY CAST(PurchaseDate AS date), ProductID;
"""

SQL_REBUILD_MONTHLY = """
DELETE FROM analytics.agg_sales_monthly;

INSERT INTO analytics.agg_sales_monthly (YearMonth, ProductID, Orders, Units, Revenue, AvgOrderValue)
SELECT
    CONVERT(char(7), CAST(PurchaseDate AS date), 120) AS YearMonth,  -- YYYY-MM
    ProductID,
    COUNT(*) AS Orders,
    SUM(PurchaseQuantity) AS Units,
    CAST(SUM(PurchaseQuantity * PurchasePrice) AS decimal(18,2)) AS Revenue,
    CAST(SUM(PurchaseQuantity * PurchasePrice) / NULLIF(COUNT(*), 0) AS decimal(18,2)) AS AvgOrderValue
FROM clean.fact_sales
GROUP BY CONVERT(char(7), CAST(PurchaseDate AS date), 120), ProductID;
"""

def rebuild_analytics() -> dict:
    """
    Rebuild analytics tables from clean.fact_sales.
    Simple + reliable for your IBP testing and demo.
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # Rebuild daily + monthly
        cur.execute(SQL_REBUILD_DAILY)
        conn.commit()

        cur.execute(SQL_REBUILD_MONTHLY)
        conn.commit()

        daily_rows = cur.execute("SELECT COUNT(*) FROM analytics.agg_sales_daily;").fetchone()[0]
        monthly_rows = cur.execute("SELECT COUNT(*) FROM analytics.agg_sales_monthly;").fetchone()[0]

    return {"daily_rows": int(daily_rows), "monthly_rows": int(monthly_rows)}
