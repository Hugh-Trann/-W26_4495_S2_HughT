use IBP

select * from dbo.upload_batch
select * from rawT.customer_review
select * from rawT.customer_purchase
select * from cleaned.dim_customer
select * from cleaned.dim_product
select * from cleaned.fact_sales

--delete from rawT.customer_review
--delete from rawT.customer_purchase
--delete from cleaned.dim_customer
--delete from cleaned.dim_product
--delete from cleaned.fact_sales
--delete from dbo.upload_batch

