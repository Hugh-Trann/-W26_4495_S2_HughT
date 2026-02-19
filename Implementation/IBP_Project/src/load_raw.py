import pandas as pd
from pathlib import Path
from src.db import get_conn

def _insert_df(cursor, table_name: str, df: pd.DataFrame):
    cols = ",".join(f"[{c}]" for c in df.columns)
    placeholders = ",".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    cursor.fast_executemany = True
    cursor.executemany(sql, df.itertuples(index=False, name=None))

def load_raw_from_uploaded_file(file_path: str, dataset_type: str) -> dict:
    """
    dataset_type: 'sales' or 'reviews'
    Loads ONE uploaded file into the correct raw table.
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Uploaded file not found: {file_path}")

    # Read file
    if p.suffix.lower() == ".csv":
        df = pd.read_csv(p)
    elif p.suffix.lower() == ".xlsx":
        df = pd.read_excel(p)
    else:
        raise ValueError("Only .csv or .xlsx supported")

    # Add metadata if your raw tables have this column
    df["_source_file"] = p.name

    # Decide target table
    if dataset_type == "sales":
        target = "raw.customer_purchase"
    elif dataset_type == "reviews":
        target = "raw.customer_review"
    else:
        raise ValueError("dataset_type must be 'sales' or 'reviews'")

    # Insert to SQL
    with get_conn() as conn:
        cur = conn.cursor()

        # IMPORTANT: do NOT TRUNCATE (safe for multi-runs)
        # If you still want dev-only truncate, you can add a flag later.

        _insert_df(cur, target, df)
        conn.commit()

        count = cur.execute(f"SELECT COUNT(*) FROM {target};").fetchone()[0]

    return {
        "target_table": target,
        "rows_inserted": len(df),
        "table_total_rows": int(count),
        "source_file": p.name,
    }
