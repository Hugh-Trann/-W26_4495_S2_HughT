import uuid
from pathlib import Path
import pandas as pd
from src.db import get_conn

SALES_COL_MAP = {
    "Order ID": "OrderId",
    "Customer Name": "CustomerName",
    "Category": "Category",
    "Sub Category": "SubCategory",
    "City": "City",
    "State": "Province",
    "Region": "Region",
    "Order Date": "OrderDate",
    "Sales": "Sales",
    "Discount": "Discount",
    "Profit": "Profit"
}

REVIEWS_COL_MAP = {
    "productId": "ProductID",
	"Title": "ProductName",
	"userId": "UserID",
	"Score": "ReviewScore",
	"Time": "ReviewDate",
	"Text": "ReviewContent",
	"Cat1":"Category",
	"Cat2":"SubCategory",
	"Cat3":"ProductType"
}

def _insert_df(cursor, table_name: str, df: pd.DataFrame):
    cols = ",".join(f"[{c}]" for c in df.columns)
    placeholders = ",".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    cursor.fast_executemany = True
    cursor.executemany(sql, df.itertuples(index=False, name=None))

def create_batch_and_load_raw(file_path: str, dataset_type: str, dataset_year: int) -> dict:
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Uploaded file not found: {file_path}")

    # check
    if p.suffix.lower() == ".csv":
        df = pd.read_csv(p)
    elif p.suffix.lower() == ".xlsx":
        df = pd.read_excel(p)
    else:
        raise ValueError("Only .csv or .xlsx supported")

    if dataset_type not in ("sales", "reviews"):
        raise ValueError("dataset_type must be 'sales' or 'reviews'")

    batch_id = str(uuid.uuid4())

    if dataset_type == "sales":
        col_map = SALES_COL_MAP
        target = "rawT.customer_purchase"
        db_cols = ["OrderId", "CustomerName", "Category", "SubCategory", "City", "Province", "Region", "OrderDate",
        "Sales", "Discount", "Profit", "_source", "batch_id", "dataset_year"
        ]
    else:
        col_map = REVIEWS_COL_MAP
        target = "rawT.customer_review"
        db_cols = ["ProductID", "ProductName", "UserID", "ReviewScore", "ReviewDate","ReviewContent", "Category", "SubCategory", "ProductType",
        "_source", "batch_id", "dataset_year"
        ]
    
    # Rename CSV columns to DB column names
    df = df.rename(columns=col_map)  

    # Add metadata columns 
    df["_source"] = p.name  
    df["batch_id"] = batch_id
    df["dataset_year"] = int(dataset_year)

    df = df[db_cols] 

    with get_conn() as conn:
        cur = conn.cursor()

        # Insert batch record
        cur.execute(
            """INSERT INTO dbo.upload_batch (batch_id, dataset_year, dataset_type, original_filename, status)
            VALUES (?, ?, ?, ?, 'uploaded');""",
            (batch_id, int(dataset_year), dataset_type, p.name),
        )
        conn.commit()

        # Insert raw rows 
        _insert_df(cur, target, df)
        conn.commit()
        
        # Check the recording result
        total_in_table = cur.execute(f"SELECT COUNT(*) FROM {target};").fetchone()[0]
        total_in_batch = cur.execute(f"SELECT COUNT(*) FROM {target} WHERE batch_id = ?;", (batch_id,)).fetchone()[0]

    return {
        "batch_id": batch_id,
        "dataset_type": dataset_type,
        "dataset_year": int(dataset_year),
        "target_table": target,
        "rows_inserted": int(total_in_batch),
        "table_total_rows": int(total_in_table),
        "source_file": p.name,
    }
