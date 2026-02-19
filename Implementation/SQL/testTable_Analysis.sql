--use IBP;

--SELECT TOP 10 * 
--FROM raw.customer_purchase

--SELECT TOP 10 *
--FROM raw.customer_review

--SELECT TOP 10 * 
--FROM clean.dim_product

--SELECT TOP 10 *
--FROM clean.dim_customer

--SELECT TOP 10 * 
--FROM clean.fact_sales

--SELECT TOP 10 *
--FROM clean.fact_review

--SELECT TOP 10 * 
--FROM analytics.agg_sales_daily
--ORDER BY SalesDate DESC;

--SELECT TOP 10 *
--FROM analytics.agg_sales_monthly
--ORDER BY YearMonth DESC;

--SELECT COUNT(*) AS clean_sales_rows FROM clean.fact_sales;
--SELECT COUNT(*) AS daily_rows FROM analytics.agg_sales_daily;
--SELECT COUNT(*) AS monthly_rows FROM analytics.agg_sales_monthly;

--EXEC sp_help 'analytics.agg_sales_daily';
--EXEC sp_help 'analytics.agg_sales_monthly';

SELECT COUNT(*) AS daily_rows FROM analytics.agg_sales_daily;
SELECT COUNT(*) AS monthly_rows FROM analytics.agg_sales_monthly;