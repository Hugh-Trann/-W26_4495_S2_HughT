USE IBP;

-- Rebuild monthly aggregates
DELETE FROM analytics.agg_sales_monthly;

INSERT INTO analytics.agg_sales_monthly (YearMonth, ProductID, Orders, Units, Revenue, AvgOrderValue)
SELECT
    CONVERT(char(7), CAST(PurchaseDate AS date), 120) AS YearMonth, -- 'YYYY-MM'
    ProductID,
    COUNT(*) AS Orders,
    SUM(PurchaseQuantity) AS Units,
    CAST(SUM(PurchaseQuantity * PurchasePrice) AS decimal(18,2)) AS Revenue,
    CAST(
        SUM(PurchaseQuantity * PurchasePrice) / NULLIF(COUNT(*), 0)
        AS decimal(18,2)
    ) AS AvgOrderValue
FROM clean.fact_sales
GROUP BY CONVERT(char(7), CAST(PurchaseDate AS date), 120), ProductID;
