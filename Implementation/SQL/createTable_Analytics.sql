use IBP;
go

create table analytics.agg_sales_daily (
        SalesDate        DATE          NOT NULL,
        ProductID        INT           NOT NULL,
        Orders           INT           NOT NULL,
        Units            INT           NOT NULL,
        Revenue          DECIMAL(18,2) NOT NULL,
        AvgOrderValue    DECIMAL(18,2) NOT NULL,
        PRIMARY KEY (SalesDate, ProductID)
);

create table analytics.agg_sales_monthly (
        YearMonth        CHAR(7)       NOT NULL, -- 'YYYY-MM'
        ProductID        INT           NOT NULL,
        Orders           INT           NOT NULL,
        Units            INT           NOT NULL,
        Revenue          DECIMAL(18,2) NOT NULL,
        AvgOrderValue    DECIMAL(18,2) NOT NULL,
        PRIMARY KEY (YearMonth, ProductID)
);

