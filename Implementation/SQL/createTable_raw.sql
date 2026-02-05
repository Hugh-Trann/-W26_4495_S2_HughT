use IBP;
go

create table raw.customer_purchase(
	TransactionID int null,
	CustomerID         INT            NULL,
    CustomerName       NVARCHAR(200)  NULL,
    ProductID          INT            NULL,
    ProductName        NVARCHAR(200)  NULL,
    ProductCategory    NVARCHAR(200)  NULL,
    PurchaseQuantity   INT            NULL,
    PurchasePrice      DECIMAL(18,2)  NULL,
    PurchaseDate       NVARCHAR(50)   NULL,  -- keep raw as text
    Country            NVARCHAR(200)  NULL,
    _ingested_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    _source_file       NVARCHAR(260)  NULL
);


create table raw.customer_review (
    ReviewID        INT            NULL,
    CustomerID      INT            NULL,
    ProductID       INT            NULL,
    ReviewText      NVARCHAR(MAX)  NULL,
    ReviewDate      NVARCHAR(50)   NULL,      -- keep raw as text
    _ingested_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    _source_file    NVARCHAR(260)  NULL
);
