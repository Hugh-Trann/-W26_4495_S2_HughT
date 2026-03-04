use IBP;
go

-- Daily sales

truncate table analytics.agg_sales_daily;

insert into analytics.agg_sales_daily (SalesDate, ProductID, Orders, Units, Revenue, AvgOrderValue)
select
    s.PurchaseDate as SalesDate,
    s.ProductID,
    COUNT(DISTINCT s.TransactionID) as Orders,
    SUM(s.PurchaseQuantity) as Units,
    SUM(s.Revenue) as Revenue,
    CAST(SUM(s.Revenue) / NULLIF(COUNT(DISTINCT s.TransactionID), 0) AS DECIMAL(18,2)) AS AvgOrderValue
from clean.fact_sales s
group by s.PurchaseDate, s.ProductID;

-- Monthly sales

truncate table analytics.agg_sales_monthly;

insert into analytics.agg_sales_monthly (YearMonth, ProductID, Orders, Units, Revenue, AvgOrderValue)
select
    CONVERT(CHAR(7), s.PurchaseDate, 120) as YearMonth,
    s.ProductID,
    COUNT(DISTINCT s.TransactionID) as Orders,
    SUM(s.PurchaseQuantity) as Units,
    SUM(s.Revenue) as Revenue,
    CAST(SUM(s.Revenue) / NULLIF(COUNT(DISTINCT s.TransactionID), 0) AS DECIMAL(18,2)) AS AvgOrderValue
from clean.fact_sales s
group by CONVERT(CHAR(7), s.PurchaseDate, 120), s.ProductID;

