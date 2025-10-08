import sqlite3
import pandas as pd
from src.reconciliation import reconcile_prices  # adjust import if needed

def test_reconcile_prices(tmp_path):
    # Setup SQLite in-memory DB
    conn = sqlite3.connect(":memory:")

    # Create dummy financial_data table
    conn.execute("""
        CREATE TABLE financial_data (
            SYMBOL TEXT,
            FUND_NAME TEXT,
            MARKET_VALUE REAL
        )
    """)

    # Insert sample data
    sample_data = [
        ("AAPL", "FundA", 1000.0),
        ("MSFT", "FundB", 2000.0),
    ]
    conn.executemany("INSERT INTO financial_data VALUES (?, ?, ?)", sample_data)
    conn.commit()

    # Create SQL file
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT * FROM financial_data")

    # Output CSV path
    output_file = tmp_path / "reconcile_output.csv"

    # Run function
    df = reconcile_prices(conn, str(output_file), str(sql_file))

    # Assertions
    assert not df.empty
    assert set(df.columns) == {"SYMBOL", "FUND_NAME", "MARKET_VALUE"}
    assert len(df) == 2

    # Check CSV was written correctly
    csv_df = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(df, csv_df)
