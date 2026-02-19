import pyodbc

SERVER = r"HTTK"
DATABASE = "IBP"
DRIVER = "ODBC Driver 17 for SQL Server"  # change to 18 if needed

CONN_STR = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

def get_conn():
    return pyodbc.connect(CONN_STR)