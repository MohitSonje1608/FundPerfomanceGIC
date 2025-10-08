import logging

import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def best_performing_funds(conn, output_file: str, sql_file: str):
    """
    Compute best performing funds by month based on Rate_of_Return.
    Formula:
        RoR = (Fund_MV_end - Fund_MV_start + Realized_P_L) / Fund_MV_start
    """

    # Step 1: Load SQL query
    if not sql_file or not os.path.exists(sql_file):
        logging.error(f"SQL file not found: {sql_file}")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    with open(sql_file, "r") as f:
        query = f.read()

    df = pd.read_sql_query(query, conn)

    if df.empty:
        logging.error("No data found in financial_data table")
        print("No data found in financial_data")
        return None

    # ---- Expected Columns: Fund_Name, Symbol, Month, MARKET_VALUE, REALISED_P_L ----
    # Ensure Month is datetime (month-level granularity)
    df["Month"] = pd.to_datetime(df["Month"]).dt.to_period("M").dt.to_timestamp()

    # Step 2: Aggregate at Fund-Month level
    fund_month_agg = (
        df.groupby(["FUND_NAME", "Month"], as_index=False)
          .agg({
              "MARKET_VALUE": "sum",
              "REALISED_P_L": "sum"
          })
          .rename(columns={
              "MARKET_VALUE": "Fund_MV_end",
              "REALISED_P_L": "Realized_P_L"
          })
    )

    # Step 3: Sort by Fund & Month
    fund_month_agg.sort_values(["FUND_NAME", "Month"], inplace=True)

    # Step 4: Compute Fund_MV_start (previous month’s end for same fund)
    fund_month_agg["Fund_MV_start"] = (
        fund_month_agg.groupby("FUND_NAME")["Fund_MV_end"].shift(1)
    )

    # Step 5: Compute Rate of Return
    fund_month_agg["Rate_of_Return"] = (
        (fund_month_agg["Fund_MV_end"]
         - fund_month_agg["Fund_MV_start"]
         + fund_month_agg["Realized_P_L"])
        / fund_month_agg["Fund_MV_start"]
    )

    # Step 6: Drop rows with NaN RoR (first month per fund)
    valid_funds_df = fund_month_agg.dropna(subset=["Rate_of_Return"])

    # Step 7: For each month, pick best performing fund
    best_per_month = valid_funds_df.loc[
        valid_funds_df.groupby("Month")["Rate_of_Return"].idxmax()
    ].reset_index(drop=True)

    # Step 8: Save to CSV
    best_per_month.to_csv(output_file, index=False)
    logging.info("Best performing funds written to {output_file}")
    print("Best performing funds report saved at {output_file}")

    return best_per_month
