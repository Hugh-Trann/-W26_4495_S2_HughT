SELECT TOP 5 * FROM raw.customer_purchase;
SELECT TOP 5 * FROM raw.customer_review;

SELECT COUNT(*) AS purchase_rows FROM raw.customer_purchase;
SELECT COUNT(*) AS review_rows   FROM raw.customer_review;


SELECT 
  COUNT(*) AS review_rows,
  SUM(CASE WHEN p.ProductID IS NULL THEN 1 ELSE 0 END) AS reviews_without_matching_product
FROM raw.customer_review r
LEFT JOIN raw.customer_purchase p
  ON r.ProductID = p.ProductID;