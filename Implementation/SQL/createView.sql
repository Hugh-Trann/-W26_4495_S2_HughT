use IBP;
go

create OR alter view analytics.v_product_sales_summary as
select
    p.ProductID,
    p.ProductName,
    p.ProductCategory,
    COUNT(DISTINCT s.TransactionID) AS Orders,
    SUM(s.PurchaseQuantity) AS Units,
    SUM(s.Revenue) AS Revenue,
    COUNT(DISTINCT r.ReviewID) AS ReviewCount
from clean.dim_product p
		LEFT JOIN clean.fact_sales s ON p.ProductID = s.ProductID
		LEFT JOIN clean.fact_review r ON p.ProductID = r.ProductID
group by p.ProductID, p.ProductName, p.ProductCategory;