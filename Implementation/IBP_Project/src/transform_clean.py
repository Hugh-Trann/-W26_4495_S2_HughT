import pandas as pd
from src.db import get_conn

def run_raw_to_clean_for_batch(batch_id: str) -> dict:
    with get_conn() as conn:
        # Read only this batch
        purchase = pd.read_sql(
            "SELECT * FROM rawT.customer_purchase WHERE batch_id = ?;",
            conn,
            params=[batch_id],
        )
        # review = pd.read_sql(
        #     "SELECT * FROM rawT.customer_review WHERE batch_id = ?;",
        #     conn,
        #     params=[batch_id],
        # )

        # basic cleaning 
        if not purchase.empty:
            for col in ["OrderId", "CustomerName", "Category", "SubCategory", "City"]:
                if col in purchase.columns:
                    purchase[col] = purchase[col].astype(str).str.strip()
            # featuring
            purchase["CustomerID"] = purchase["CustomerName"] + purchase["City"]
            purchase["CustomerID"] = purchase["CustomerID"].str.replace(r"[\s,.\-_]+", "", regex=True)
            purchase["ProductID"] = purchase["Category"] + purchase["SubCategory"]
            purchase["ProductID"] = purchase["ProductID"].str.replace(r"[\s,.\-_]+", "", regex=True)
            purchase["OderId"] = purchase["OrderId"].astype(str)
            purchase["OrderDate"] = pd.to_datetime(purchase["OrderDate"], errors="coerce")
            purchase["Sales"] = pd.to_numeric(purchase["Sales"], errors="coerce")
            purchase["Discount"] = pd.to_numeric(purchase["Discount"], errors="coerce")
            purchase["Profit"] = pd.to_numeric(purchase["Profit"], errors="coerce")
        

        # create dims from purchase
        dim_customer = pd.DataFrame(columns=["CustomerID", "CustomerName", "City"])
        dim_customer["batch_id"] = batch_id
        dim_product = pd.DataFrame(columns=["ProductID", "Category", "SubCategory"])
        dim_product["batch_id"] = batch_id

        if not purchase.empty:
            dim_customer = (
                purchase[["CustomerID", "CustomerName", "City"]]
                # .dropna(subset=["CustomerID"])
                # .assign(CustomerID=lambda d: d["CustomerID"].astype(str))
                .sort_values(["CustomerID"])
                .drop_duplicates(subset=["CustomerID"], keep="first")
            )
            
            dim_product = (
                purchase[["ProductID", "Category", "SubCategory"]]
                # .dropna(subset=["ProductID"])
                # .assign(ProductID=lambda d: d["ProductID"].astype(str))
                .sort_values(["ProductID"])
                .drop_duplicates(subset=["ProductID"], keep="first")
            )
            

        # create fact from purchase
        fact_sales = pd.DataFrame()
        if not purchase.empty:
            fact_sales = purchase[["OrderId", "CustomerID", "ProductID", "OrderDate", "Sales", "Discount", "Profit"]].copy()
            fact_sales["batch_id"] = batch_id

        cur = conn.cursor()
        cur.fast_executemany = True

        # Customers
        if not dim_customer.empty:
            cur.executemany(
                """
                INSERT INTO cleaned.dim_customer (CustomerID, CustomerName, CustomerCity, batch_id)
                SELECT ?, ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM cleaned.dim_customer WHERE CustomerID = ?);
                """,
                [(r.CustomerID, r.CustomerName, r.City, batch_id, r.CustomerID) for r in dim_customer.itertuples(index=False)]
            )

        # Products
        if not dim_product.empty:
            cur.executemany(
                """
                INSERT INTO cleaned.dim_product (ProductID, Category, SubCategory, batch_id)
                SELECT ?, ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM cleaned.dim_product WHERE ProductID = ?);
                """,
                [(r.ProductID, r.Category, r.SubCategory, batch_id, r.ProductID) for r in dim_product.itertuples(index=False)]
            )

        conn.commit()

        # cur.execute("DELETE FROM clean.fact_sales  WHERE batch_id = ?;", (batch_id,))
        # conn.commit()

        if not fact_sales.empty:
            cur.executemany(
                """
                INSERT INTO cleaned.fact_sales
                (OrderID, CustomerID, ProductID, OrderDate, Sales, Discount, Profit, batch_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                fact_sales.itertuples(index=False, name=None)
            )

        # if not fact_review.empty:
        #     cur.executemany(
        #         """
        #         INSERT INTO clean.fact_review
        #         (ReviewID, CustomerID, ProductID, ReviewDate, ReviewText, batch_id)
        #         VALUES (?, ?, ?, ?, ?, ?);
        #         """,
        #         fact_review.itertuples(index=False, name=None)
        #     )

        conn.commit()

        return {
            "batch_id": batch_id,
            "fact_sales_rows": int(len(fact_sales)),
            # "fact_review_rows": int(len(fact_review)),
            "dim_customer_count": int(len(dim_customer)),
            "dim_product_count": int(len(dim_product)),
        }
