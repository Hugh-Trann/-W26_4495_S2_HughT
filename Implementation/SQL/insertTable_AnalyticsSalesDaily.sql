USE IBP;

-- Rebuild daily aggregates
DELETE FROM analytics.agg_sales_daily;

INSERT INTO analytics.agg_sales_daily (SalesDate, ProductID, Orders, Units, Revenue, AvgOrderValue)
SELECT
    CAST(PurchaseDate AS date) AS SalesDate,
    ProductID,
    COUNT(*) AS Orders,  -- or COUNT(DISTINCT TransactionID) if TransactionID can repeat
    SUM(PurchaseQuantity) AS Units,
    CAST(SUM(PurchaseQuantity * PurchasePrice) AS decimal(18,2)) AS Revenue,
    CAST(
        SUM(PurchaseQuantity * PurchasePrice) / NULLIF(COUNT(*), 0)
        AS decimal(18,2)
    ) AS AvgOrderValue
FROM clean.fact_sales
GROUP BY CAST(PurchaseDate AS date), ProductID;