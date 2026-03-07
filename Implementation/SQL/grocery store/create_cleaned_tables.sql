use IBP

create schema cleaned
go

create table cleaned.dim_customer(
	CustomerID varchar(200) not null primary key,
	CustomerName varchar(100) not null,
	CustomerCity varchar(100) not null
);

create table cleaned.dim_product(
	ProductID varchar(200) not null primary key,
	Category varchar(100) not null,
	SubCategory varchar(100) not null
);

create table cleaned.fact_sales(
	OrderID varchar(20) not null primary key,
	ProductID varchar(200) not null,
	CustomerID varchar(200) not null,
	OrderDate date not null,
	Sales decimal(10,2) not null,
	Discount decimal(8,2) not null,
	Profit decimal(10,2) not null,
	constraint FK_fact_sales_customer foreign key(CustomerID) references cleaned.dim_customer(CustomerID),
	constraint FK_fact_sales_product foreign key(ProductID) references cleaned.dim_product(ProductID)
);
