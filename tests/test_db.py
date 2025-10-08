import os
import sqlite3
from src.db_utils import get_connection

def test_connection(tmp_path):
    db_file = tmp_path / "test.db"
    conn = get_connection(str(db_file))
    assert isinstance(conn, sqlite3.Connection)
    conn.close()
