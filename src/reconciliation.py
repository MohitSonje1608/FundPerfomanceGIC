import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def reconcile_prices(conn, output_file: str, sql_file: str ):
    """
    Compare fund report prices vs reference data prices (bond/equity).
    """
    if sql_file and os.path.exists(sql_file):
        with open(sql_file, "r") as f:
            query = f.read()
    else:
        logging.error(f"SQL file not found: {sql_file}")
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    df = pd.read_sql_query(query, conn)
    df.to_csv(output_file, index=False)
    return df
