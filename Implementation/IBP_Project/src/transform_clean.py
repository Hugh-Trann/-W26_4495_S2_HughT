import pandas as pd
from src.db import get_conn

def run_raw_to_clean_for_batch(batch_id: str) -> dict:
    with get_conn() as conn:
        # Read only this batch
        purchase = pd.read_sql(
            "SELECT * FROM raw.customer_purchase WHERE batch_id = ?;",
            conn,
            params=[batch_id],
        )
        review = pd.read_sql(
            "SELECT * FROM raw.customer_review WHERE batch_id = ?;",
            conn,
            params=[batch_id],
        )

        # Basic cleaning (keep your existing logic) :contentReference[oaicite:5]{index=5}
        if not purchase.empty:
            for col in ["CustomerName", "Country", "ProductName", "ProductCategory"]:
                if col in purchase.columns:
                    purchase[col] = purchase[col].astype(str).str.strip()

        # Build dims from purchase (global dims)
        dim_customer = pd.DataFrame(columns=["CustomerID", "CustomerName", "Country"])
        dim_product = pd.DataFrame(columns=["ProductID", "ProductName", "ProductCategory"])

        if not purchase.empty:
            dim_customer = (
                purchase[["CustomerID", "CustomerName", "Country"]]
                .dropna(subset=["CustomerID"])
                .assign(CustomerID=lambda d: d["CustomerID"].astype(int))
                .sort_values(["CustomerID"])
                .drop_duplicates(subset=["CustomerID"], keep="first")
            )
            dim_product = (
                purchase[["ProductID", "ProductName", "ProductCategory"]]
                .dropna(subset=["ProductID"])
                .assign(ProductID=lambda d: d["ProductID"].astype(int))
                .sort_values(["ProductID"])
                .drop_duplicates(subset=["ProductID"], keep="first")
            )

        # Facts (batch-specific)
        fact_sales = pd.DataFrame()
        if not purchase.empty:
            fact_sales = purchase[[
                "TransactionID", "CustomerID", "ProductID",
                "PurchaseDate", "PurchaseQuantity", "PurchasePrice"
            ]].copy()
            fact_sales["batch_id"] = batch_id

        fact_review = pd.DataFrame()
        if not review.empty:
            fact_review = review[[
                "ReviewID", "CustomerID", "ProductID",
                "ReviewDate", "ReviewText"
            ]].copy()
            fact_review["batch_id"] = batch_id

        cur = conn.cursor()
        cur.fast_executemany = True

        # 1) Upsert dims (insert only missing)
        # Customers
        if not dim_customer.empty:
            cur.executemany(
                """
                INSERT INTO clean.dim_customer (CustomerID, CustomerName, Country)
                SELECT ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM clean.dim_customer WHERE CustomerID = ?);
                """,
                [(r.CustomerID, r.CustomerName, r.Country, r.CustomerID)
                 for r in dim_customer.itertuples(index=False)]
            )

        # Products
        if not dim_product.empty:
            cur.executemany(
                """
                INSERT INTO clean.dim_product (ProductID, ProductName, ProductCategory)
                SELECT ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM clean.dim_product WHERE ProductID = ?);
                """,
                [(r.ProductID, r.ProductName, r.ProductCategory, r.ProductID)
                 for r in dim_product.itertuples(index=False)]
            )

        conn.commit()

        # 2) Delete only this batch from facts, then insert batch facts
        cur.execute("DELETE FROM clean.fact_sales  WHERE batch_id = ?;", (batch_id,))
        cur.execute("DELETE FROM clean.fact_review WHERE batch_id = ?;", (batch_id,))
        conn.commit()

        if not fact_sales.empty:
            cur.executemany(
                """
                INSERT INTO clean.fact_sales
                (TransactionID, CustomerID, ProductID, PurchaseDate, PurchaseQuantity, PurchasePrice, batch_id)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                fact_sales.itertuples(index=False, name=None)
            )

        if not fact_review.empty:
            cur.executemany(
                """
                INSERT INTO clean.fact_review
                (ReviewID, CustomerID, ProductID, ReviewDate, ReviewText, batch_id)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                fact_review.itertuples(index=False, name=None)
            )

        conn.commit()

        return {
            "batch_id": batch_id,
            "fact_sales_rows": int(len(fact_sales)),
            "fact_review_rows": int(len(fact_review)),
            "dim_customer_inferred": int(len(dim_customer)),
            "dim_product_inferred": int(len(dim_product)),
        }
