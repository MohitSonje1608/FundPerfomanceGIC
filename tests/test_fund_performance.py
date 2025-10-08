import sqlite3
import pandas as pd
from src.fund_performance import best_performing_funds

def test_best_performing_funds(tmp_path):
    # Setup SQLite in-memory DB
    conn = sqlite3.connect(":memory:")

    # Create dummy financial_data table
    conn.execute("""
        CREATE TABLE financial_data (
            SYMBOL TEXT,
            FUND_NAME TEXT,
            MARKET_VALUE REAL,
            REALISED_P_L REAL,
            Month TEXT
        )
    """)

    # Insert mock data for 2 funds across 2 months
    sample_data = [
        # FundA Jan
        ("AAPL", "FundA", 1000, 50, "2023-01-01"),
        ("MSFT", "FundA", 1500, 100, "2023-01-01"),
        # FundA Feb
        ("AAPL", "FundA", 2000, 80, "2023-02-01"),
        ("MSFT", "FundA", 1800, 90, "2023-02-01"),

        # FundB Jan
        ("GOOG", "FundB", 1200, 40, "2023-01-01"),
        ("TSLA", "FundB", 1600, 70, "2023-01-01"),
        # FundB Feb
        ("GOOG", "FundB", 2500, 100, "2023-02-01"),
        ("TSLA", "FundB", 3000, 120, "2023-02-01"),
    ]
    conn.executemany(
        "INSERT INTO financial_data VALUES (?, ?, ?, ?, ?)", sample_data
    )
    conn.commit()

    # Create a temporary SQL file to fetch data
    sql_file = tmp_path / "funds_query.sql"
    sql_file.write_text("SELECT * FROM financial_data")

    # Output CSV path
    output_file = tmp_path / "best_funds.csv"

    # Run analysis
    result_df = best_performing_funds(conn, str(output_file), str(sql_file))

    # Assertions
    assert not result_df.empty
    assert "Rate_of_Return" in result_df.columns
    assert "FUND_NAME" in result_df.columns

    # Extract results by month
    results = dict(zip(result_df["Month"].dt.strftime("%Y-%m"), result_df["FUND_NAME"]))

    # EXPECTED: FundB wins Feb (since Jan is skipped due to no RoR)
    assert results["2023-02"] == "FundB"
