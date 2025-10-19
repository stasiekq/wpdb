#!/usr/bin/env python3
# business_process.py
# Wczytuje 3 CSVy z katalogu data/, tworzy tabele w PostgreSQL i wstawia dane.
# Komunikaty konsoli bez polskich znakow.

import os
import sys
from io import StringIO
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()  # wczytuje .env jesli jest

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "business_db")
DB_USER = os.getenv("POSTGRES_USER", "pguser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")

DATA_DIR = "data"  # katalog z csv

TYPE_MAP = {
    'int64': 'INTEGER',
    'float64': 'REAL',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP',
    'object': 'TEXT'
}

def infer_sql_type(pd_series):
    dtype = str(pd_series.dtype)
    return TYPE_MAP.get(dtype, 'TEXT')

def table_name_from_filename(fname):
    base = os.path.splitext(os.path.basename(fname))[0]
    # usun niebezpieczne znaki, zamien na underscore
    return ''.join(c if c.isalnum() else '_' for c in base).lower()

def create_table(conn, table_name, df):
    cols = []
    for col in df.columns:
        sql_type = infer_sql_type(df[col])
        safe_col = sql.Identifier(col)
        cols.append(f"{col} {sql_type}")
    cols_sql = ", ".join(cols)
    create = f"CREATE TABLE IF NOT EXISTS {sql.Identifier(table_name).string} ({cols_sql});"
    # psycopg2 sql building for safety:
    with conn.cursor() as cur:
        # build column defs safely using sql.Identifier for names
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

def copy_df_to_table(conn, table_name, df):
    # use COPY FROM STDIN for speed
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    with conn.cursor() as cur:
        cols = [sql.Identifier(c) for c in df.columns]
        cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(cols)
        ), buffer)
    conn.commit()

def process_all_csvs():
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv")]
    if not files:
        print("Brak plikow CSV w katalogu 'data'.")
        return

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    try:
        for fpath in files:
            print(f"Przetwarzam: {fpath}")
            df = pd.read_csv(fpath)
            # prosta normalizacja nazw kolumn: usun spacje
            df.columns = [c.strip().replace(' ', '_') for c in df.columns]
            table = table_name_from_filename(fpath)
            print(f"Tworze tabele: {table}")
            create_table(conn, table, df)
            print(f"Wstawiam dane do: {table} ({len(df)} wierszy)")
            copy_df_to_table(conn, table, df)
            print("OK")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        process_all_csvs()
        print("Zakonczono przetwarzanie.")
    except Exception as e:
        print("Blad podczas przetwarzania:", e)
        sys.exit(1)
