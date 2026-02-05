use IBP;
go

SELECT COUNT(*) customers FROM clean.dim_customer;
SELECT COUNT(*) products  FROM clean.dim_product;
SELECT COUNT(*) sales     FROM clean.fact_sales;
SELECT COUNT(*) reviews   FROM clean.fact_review;

-- Check duplicates (should be 0)
SELECT CustomerID, COUNT(*) c
FROM clean.dim_customer
GROUP BY CustomerID
HAVING COUNT(*) > 1;