import pandas as pd
import pyodbc

SERVER = r"HTTK"
DATABASE = "IBP"
DRIVER = "ODBC Driver 17 for SQL Server"

conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

with pyodbc.connect(conn_str) as conn:
    # ---------- Read RAW ----------
    purchase = pd.read_sql("SELECT * FROM raw.customer_purchase;", conn)
    review   = pd.read_sql("SELECT * FROM raw.customer_review;", conn)

    # Clean strings
    purchase["CustomerName"] = purchase["CustomerName"].astype(str).str.strip()
    purchase["Country"] = purchase["Country"].astype(str).str.strip()
    purchase["ProductName"] = purchase["ProductName"].astype(str).str.strip()
    purchase["ProductCategory"] = purchase["ProductCategory"].astype(str).str.strip()

    # Combine IDs from both tables
    all_customer_ids = pd.concat([purchase["CustomerID"], review["CustomerID"]]).dropna().astype(int).drop_duplicates()
    all_product_ids  = pd.concat([purchase["ProductID"], review["ProductID"]]).dropna().astype(int).drop_duplicates()

    # dim_customer from purchase (best available attributes), constrained to known IDs
    dim_customer = (
        purchase[["CustomerID", "CustomerName", "Country"]]
        .dropna(subset=["CustomerID"])
        .assign(CustomerID=lambda d: d["CustomerID"].astype(int))
        .sort_values(["CustomerID"])
        .drop_duplicates(subset=["CustomerID"], keep="first")
    )
    dim_customer = dim_customer[dim_customer["CustomerID"].isin(all_customer_ids)]

    # dim_product from purchase
    dim_product = (
        purchase[["ProductID", "ProductName", "ProductCategory"]]
        .dropna(subset=["ProductID"])
        .assign(ProductID=lambda d: d["ProductID"].astype(int))
        .sort_values(["ProductID"])
        .drop_duplicates(subset=["ProductID"], keep="first")
    )
    dim_product = dim_product[dim_product["ProductID"].isin(all_product_ids)]

    fact_sales = purchase[[
        "TransactionID", "CustomerID", "ProductID",
        "PurchaseDate", "PurchaseQuantity", "PurchasePrice"
    ]]

    fact_review = review[[
        "ReviewID", "CustomerID", "ProductID",
        "ReviewDate", "ReviewText"
    ]]

    cur = conn.cursor()
    cur.fast_executemany = True

    # ---------- Clear CLEAN ----------
    cur.execute("DELETE FROM clean.fact_review;")
    cur.execute("DELETE FROM clean.fact_sales;")
    cur.execute("DELETE FROM clean.dim_product;")
    cur.execute("DELETE FROM clean.dim_customer;")
    conn.commit()

    # ---------- Insert DIM ----------
    cur.executemany(
        "INSERT INTO clean.dim_customer (CustomerID, CustomerName, Country) VALUES (?, ?, ?);",
        dim_customer.itertuples(index=False, name=None)
    )

    cur.executemany(
        "INSERT INTO clean.dim_product (ProductID, ProductName, ProductCategory) VALUES (?, ?, ?);",
        dim_product.itertuples(index=False, name=None)
    )
    conn.commit()

    # ---------- Insert FACT ----------
    cur.executemany(
        "INSERT INTO clean.fact_sales (TransactionID, CustomerID, ProductID, PurchaseDate, PurchaseQuantity, PurchasePrice) VALUES (?, ?, ?, ?, ?, ?);",
        fact_sales.itertuples(index=False, name=None)
    )

    cur.executemany(
        "INSERT INTO clean.fact_review (ReviewID, CustomerID, ProductID, ReviewDate, ReviewText) VALUES (?, ?, ?, ?, ?);",
        fact_review.itertuples(index=False, name=None)
    )
    conn.commit()

    print("RAW => CLEAN transformation completed")