USE IBP;

CREATE TABLE dbo.upload_batch (
    batch_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    dataset_year INT NOT NULL,
    dataset_type VARCHAR(20) NOT NULL,
    original_filename NVARCHAR(255) NOT NULL,
    uploaded_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    status VARCHAR(20) NOT NULL DEFAULT 'uploaded'
);
