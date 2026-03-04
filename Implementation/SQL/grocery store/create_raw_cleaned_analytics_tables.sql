use IBP
go

create schema raw
go

create schema cleaned
go

create schema analytics
go

create table raw.customer_purchase(
	OrderId varchar(20) not null,
	CustomerName varchar(50) null,
	Category varchar(50) null,
	SubCategory varchar(50) null,
	City varchar(50) null,
	Province varchar(50) null,
	Region varchar(50) null,
	OrderDate date not null,
	Sales decimal(10,2) null,
	Discount decimal(8,2) null,
	Profit decimal(10,2) null,
	_ingested_at date not null default sysutcdatetime(),
	_source varchar(250) null
);

create table raw.customer_review (
	ProductID varchar(50) null ,
	ProductName varchar(150) null,
	UserID varchar(50) not null primary key,
	ReviewScore int null,
	ReviewDate datetime null,
	ReviewContent varchar(max) null,
	Category varchar(100) null,
	SubCategory varchar(100) null,
	ProductType varchar(100) null,
	_ingested_at date not null default sysutcdatetime(),
	_source varchar(250) null
);

create table cleaned.dim_product(
	ProductID varchar(200) not null primary key,
	Category varchar(100) null,
	ProductType varchar(100) null,
	created_at date not null default sysutcdatetime(),
	updated_at date not null default sysutcdatetime()
);

create table cleaned.dim_customer(
	CustomerID varchar(200) not null primary key,
	CustomerName varchar(100),
	created_at date not null default sysutcdatetime(),
	updated_at date not null default sysutcdatetime()
);

create table cleaned.fact_sales(
	OrderId varchar(20) not null primary key,
	CustomerID varchar(200) null,
	ProductID varchar(200) null,
	OrderDate date null,
	SaleAmount float null,
	Discount float null,
	Profit float null,
	constraint FK_Fact_Sales_Customer foreign key(CustomerID) references cleaned.dim_customer(CustomerID),
	constraint FK_Fact_Sales_Product foreign key(ProductID) references cleaned.dim_product(ProductID),
	created_at date not null default sysutcdatetime(),
	updated_at date not null default sysutcdatetime()
);

create table analytics.agg_sales_daily(
	SalesDate date not null,
	OrderID varchar(20) not null,
	ProductID varchar(200) not null,
	Revenue decimal(18,2) not null,
	Discount decimal(18,2) not null,
	Profit decimal(18,2) not null,
	AvgOrderValue decimal(18,2) not null,
	primary key(SalesDate, ProductID)
);

create table analytics.agg_sales_monthly(
	YearMonth char(7) not null,
	OrderId varchar(20) not null,
	ProductID varchar(200) not null,
	Revenue decimal(18,2) not null,
	Discount decimal(18,2) not null,
	Profit decimal(18,2) not null,
	AvgOrderValue decimal(18,2) not null,
	primary key(YearMonth, ProductID)
);

