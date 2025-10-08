import os
import re
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

# ---------------- Logging Setup ----------------
logger = logging.getLogger(__name__)

# ---------------- Constants ----------------
_DATE_PATTERNS = [
    r'\d{4}[-_]\d{2}[-_]\d{2}',  # 2023-02-28 or 2023_02_28
    r'\d{2}[-_]\d{2}[-_]\d{4}',  # 28-02-2023 or 01-31-2023
    r'\d{8}',                    # 20220831
]

_PREFIX_SUFFIX_NOISE = {
    'fund', 'report', 'rpt', 'tt', 'tt_monthly', 'monthly', 'mend',
    'mend-report', 'report-of', 'of', 'the', 'reportof'
}
_SUFFIX_WORDS_TO_STRIP = {'breakdown', 'details', 'securities', 'report'}

# ---------------- Helper Functions ----------------
def _find_date_substring(base: str):
    """Return the first matching date substring or None."""
    for pat in _DATE_PATTERNS:
        m = re.search(pat, base)
        if m:
            return m.group()
    return None

def _parse_date_token(token: str):
    """Try to parse the token into a datetime and return YYYY-MM or None."""
    if not token:
        return None
    token_norm = token.replace('_', '-')
    fmts = ["%d-%m-%Y", "%m-%d-%Y", "%Y-%m-%d", "%Y%m%d"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(token_norm, fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            continue
    return None

def _clean_candidate_name(candidate: str):
    """
    Clean a candidate fund-name string.
    """
    s = re.sub(r'[._\-\s]+', ' ', candidate).strip()
    s = re.sub(
        r'\b(' + '|'.join(re.escape(w) for w in _SUFFIX_WORDS_TO_STRIP) + r')\b',
        ' ', s, flags=re.I
    ).strip()

    tokens = [t for t in s.split() if re.search('[A-Za-z]', t)]
    tokens = [t for t in tokens if t.lower() not in _PREFIX_SUFFIX_NOISE]

    if not tokens:
        tokens = [t for t in re.split(r'[._\-\s]+', candidate) if re.search('[A-Za-z]', t)]
        tokens = [t for t in tokens if t.lower() not in _PREFIX_SUFFIX_NOISE]

    if not tokens:
        return "Unknown"

    return " ".join(tokens).strip().title()

def extract_month_and_fund_from_filename(filename: str):
    """Returns (month_str_or_None, fund_name_str)."""
    base = os.path.splitext(filename)[0]

    date_token = _find_date_substring(base)
    month = _parse_date_token(date_token) if date_token else None

    if date_token:
        candidate = re.sub(re.escape(date_token), ' ', base, count=1)
    else:
        candidate = base

    fund_name = _clean_candidate_name(candidate)
    return month, fund_name

# ---------------- CSV Loader ----------------
def load_csv_data(conn, csv_dir: str, table_name: str) -> None:
    """Load all CSV files into table, adding Month and Fund_Name columns."""
    for file_path in Path(csv_dir).glob("*.csv"):
        filename = file_path.name
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            logger.exception(f"Skipping {filename} — could not read CSV: {e}")
            continue

        df.columns = (df.columns
                        .str.strip()
                        .str.replace(" ", "_", regex=False)
                        .str.replace("P/L", "P_L", regex=False))

        if "SEDOL" not in df.columns and "ISIN" not in df.columns:
            # If neither exists, just add a blank column for consistency
            df["SEDOL"] = None
        elif "ISIN" in df.columns and "SEDOL" not in df.columns:
            # If only ISIN present, keep ISIN as-is but also add blank SEDOL
            df["SEDOL"] = None
        elif "SEDOL" in df.columns and "ISIN" not in df.columns:
            # If only SEDOL present, keep it (bonds case)
            pass

        month_val, fund_name = extract_month_and_fund_from_filename(filename)
        df["Month"] = month_val
        df["Fund_Name"] = fund_name
        df = df.drop_duplicates()

        # Write to DB
        try:
            df.to_sql(table_name, conn, if_exists="append", index=False, chunksize=500)
            logger.info(f"Inserted {len(df)} rows from {filename} into {table_name}.")
        except Exception as e:
            logger.exception(f"Failed to insert {filename}: {e}")
