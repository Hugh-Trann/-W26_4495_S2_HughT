USE IBP;

DECLARE @legacy_batch UNIQUEIDENTIFIER = NEWID();

UPDATE raw.customer_purchase SET batch_id = @legacy_batch WHERE batch_id IS NULL;
UPDATE raw.customer_review   SET batch_id = @legacy_batch WHERE batch_id IS NULL;

UPDATE clean.fact_sales  SET batch_id = @legacy_batch WHERE batch_id IS NULL;
UPDATE clean.fact_review SET batch_id = @legacy_batch WHERE batch_id IS NULL;

ALTER TABLE raw.customer_purchase ALTER COLUMN batch_id UNIQUEIDENTIFIER NOT NULL;
ALTER TABLE raw.customer_review   ALTER COLUMN batch_id UNIQUEIDENTIFIER NOT NULL;

ALTER TABLE clean.fact_sales  ALTER COLUMN batch_id UNIQUEIDENTIFIER NOT NULL;
ALTER TABLE clean.fact_review ALTER COLUMN batch_id UNIQUEIDENTIFIER NOT NULL;






