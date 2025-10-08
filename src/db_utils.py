import sqlite3
import os

def get_connection(db_name: str):
    """Return a SQLite connection."""
    return sqlite3.connect(db_name)

def execute_sql_file(conn, sql_file_path: str):
    """Execute all statements from an SQL file."""
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
    with open(sql_file_path, "r") as f:
        sql_script = f.read()
    conn.executescript(sql_script)
    conn.commit()
