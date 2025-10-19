import os
import sys
from io import StringIO
from typing import List


import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv


# Load .env if present
load_dotenv()


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "business_db")
DB_USER = os.getenv("POSTGRES_USER", "pguser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")


DATA_DIR = os.getenv("DATA_DIR", "data") # katalog z csv
MAX_FILES = 3 # czytamy do 3 plikow (zadanie wymaga 3 CSV)


TYPE_MAP = {
'int64': 'INTEGER',
'float64': 'REAL',
'bool': 'BOOLEAN',
'datetime64[ns]': 'TIMESTAMP',
'object': 'TEXT'
}




def infer_sql_type(pd_series: pd.Series) -> str:
    dtype = str(pd_series.dtype)
    return TYPE_MAP.get(dtype, 'TEXT')




def table_name_from_filename(fname: str) -> str:
    base = os.path.splitext(os.path.basename(fname))[0]
    # usun niebezpieczne znaki, zamien na underscore
    return ''.join(c if c.isalnum() else '_' for c in base).lower()



def create_table(conn, table_name: str, df: pd.DataFrame):
# build column defs safely using psycopg2.sql
    with conn.cursor() as cur:
        col_defs = []
        for col in df.columns:
            col_name = sql.Identifier(col)
            col_type = sql.SQL(infer_sql_type(df[col]))
            col_defs.append(sql.SQL("{} {}").format(col_name, col_type))
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ( {} );").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(col_defs)
        )
        cur.execute(query)
    conn.commit()



def copy_df_to_table(conn, table_name: str, df: pd.DataFrame):
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql_str = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cur.execute(sql_str, tuple(row))
    conn.commit()




def find_csv_files(data_dir: str, max_files: int = 3) -> List[str]:
    if not os.path.isdir(data_dir):
        return []
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.lower().endswith('.csv')]
    files.sort()
    return files[:max_files]


def process_csv_list(file_list: List[str]):
    if not file_list:
        print(f"Brak plikow CSV w katalogu '{DATA_DIR}'.")
        return

    print(f"Liczba plikow do przetworzenia: {len(file_list)}")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    try:
        for fpath in file_list:
            print(f"Przetwarzam: {fpath}")
            df = pd.read_csv(fpath)
            # prosta normalizacja nazw kolumn: usun spacje, zamien na underscore
            df.columns = [c.strip().replace(' ', '_') for c in df.columns]
            table = table_name_from_filename(fpath)
            print(f"Tworze tabele: {table}")
            create_table(conn, table, df)
            print(f"Wstawiam dane do: {table} ({len(df)} wierszy)")
            copy_df_to_table(conn, table, df)
            print("OK")
    finally:
        conn.close()


if __name__ == '__main__':
    # mozna podac pliki jako argumenty: python business_process_task1.py data/a.csv data/b.csv data/c.csv
    if len(sys.argv) > 1:
        files = [p for p in sys.argv[1:] if p.lower().endswith('.csv')]
    else:
        files = find_csv_files(DATA_DIR, MAX_FILES)


    if len(files) < 3:
        print(f"Uwaga: znaleziono tylko {len(files)} pliki CSV. Skrypt sprobuje przetworzyc dostepne pliki.")


    try:
        process_csv_list(files)
        print("Zakonczono przetwarzanie.")
    except Exception as e:
        print("Blad podczas przetwarzania:", e)
        sys.exit(1)