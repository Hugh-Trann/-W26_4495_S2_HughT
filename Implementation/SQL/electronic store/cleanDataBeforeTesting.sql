USE IBP;

-- ANALYTICS
DELETE FROM analytics.agg_sales_daily;
DELETE FROM analytics.agg_sales_monthly;

-- CLEAN
DELETE FROM clean.fact_review;
DELETE FROM clean.fact_sales;
DELETE FROM clean.dim_product;
DELETE FROM clean.dim_customer;

-- RAW
DELETE FROM raw.customer_review;
DELETE FROM raw.customer_purchase;

-- BATCH
DELETE FROM dbo.upload_batch;

-- TEST
SELECT COUNT(*) FROM raw.customer_purchase;
SELECT COUNT(*) FROM clean.fact_sales;
SELECT COUNT(*) FROM dbo.upload_batch;

--SELECT * FROM dbo.upload_batch;
--SELECT dataset_year, COUNT(*) FROM raw.customer_purchase GROUP BY dataset_year;
--SELECT batch_id, COUNT(*) FROM clean.fact_sales GROUP BY batch_id;