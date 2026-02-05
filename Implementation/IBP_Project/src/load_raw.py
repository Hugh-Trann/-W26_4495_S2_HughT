import pandas as pd
import pyodbc 
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "dataset"

PURCHASE_CSV = DATA_DIR / "customer_purchase_data.csv"
REVIEWS_CSV  = DATA_DIR / "customer_reviews_data.csv"

# SQL Server connection (Windows Auth)
SERVER = r"HTTK"
DATABASE = "IBP"
DRIVER = "ODBC Driver 17 for SQL Server"  # change to 18 if needed

conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

def insert_df(cursor, table_name: str, df: pd.DataFrame):
    """Insert dataframe rows into SQL table (column order must match)."""
    cols = ",".join(f"[{c}]" for c in df.columns)
    placeholders = ",".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    cursor.fast_executemany = True
    cursor.executemany(sql, df.itertuples(index=False, name=None))

def main():
    # Read CSVs
    purchase_df = pd.read_csv(PURCHASE_CSV)
    reviews_df  = pd.read_csv(REVIEWS_CSV)

    # Add metadata column that exists in raw tables
    purchase_df["_source_file"] = PURCHASE_CSV.name
    reviews_df["_source_file"]  = REVIEWS_CSV.name

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()

        # Dev-only: clear raw tables
        cur.execute("TRUNCATE TABLE raw.customer_purchase;")
        cur.execute("TRUNCATE TABLE raw.customer_review;")
        conn.commit()

        # Insert
        insert_df(cur, "raw.customer_purchase", purchase_df)
        insert_df(cur, "raw.customer_review", reviews_df)
        conn.commit()

        # Row counts
        n1 = cur.execute("SELECT COUNT(*) FROM raw.customer_purchase;").fetchone()[0]
        n2 = cur.execute("SELECT COUNT(*) FROM raw.customer_review;").fetchone()[0]
        print("Loaded raw.customer_purchase rows:", n1)
        print("Loaded raw.customer_review rows:", n2)

if __name__ == "__main__":
    main()
