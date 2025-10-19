import os
from dotenv import load_dotenv

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "business_db")
DB_USER = os.getenv("POSTGRES_USER", "pguser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")

DATA_DIR = os.getenv("DATA_DIR", "data") 

load_dotenv()

def wipe_all_tables():
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public';
        """)
        tables = cur.fetchall()
        for (table_name,) in tables:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name)))
            print(f"Dropped table {table_name}")

    conn.close()

if __name__ == "__main__":
    wipe_all_tables()