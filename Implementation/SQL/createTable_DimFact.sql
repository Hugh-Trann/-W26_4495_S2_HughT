use IBP;
go


create table clean.dim_customer (
    CustomerID     INT           NOT NULL PRIMARY KEY,
    CustomerName   NVARCHAR(200) NULL,
    Country        NVARCHAR(200) NULL,
    created_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);


create table clean.dim_product (
    ProductID        INT           NOT NULL PRIMARY KEY,
    ProductName      NVARCHAR(200) NULL,
    ProductCategory  NVARCHAR(200) NULL,
    created_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);



create table clean.fact_sales (
    TransactionID     INT           NOT NULL PRIMARY KEY,
    CustomerID        INT           NOT NULL,
    ProductID         INT           NOT NULL,
    PurchaseDate      DATE          NOT NULL,
    PurchaseQuantity  INT           NOT NULL,
    PurchasePrice     DECIMAL(18,2) NOT NULL,
    Revenue           AS (PurchaseQuantity * PurchasePrice) PERSISTED,

    constraint FK_fact_sales_customer
        FOREIGN KEY (CustomerID) REFERENCES clean.dim_customer(CustomerID),

    constraint FK_fact_sales_product
        FOREIGN KEY (ProductID)  REFERENCES clean.dim_product(ProductID)
);


create table clean.fact_review (
    ReviewID     INT           NOT NULL PRIMARY KEY,
    CustomerID   INT           NOT NULL,
    ProductID    INT           NOT NULL,
    ReviewDate   DATE          NOT NULL,
    ReviewText   NVARCHAR(MAX) NOT NULL,

    constraint FK_fact_review_customer
        FOREIGN KEY (CustomerID) REFERENCES clean.dim_customer(CustomerID),

    constraint FK_fact_review_product
        FOREIGN KEY (ProductID)  REFERENCES clean.dim_product(ProductID)
);
