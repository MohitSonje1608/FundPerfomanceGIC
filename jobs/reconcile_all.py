# jobs/reconcile_all.py
import os
import sys
import logging
from src.main import read_config
from src.db_utils import get_connection
from src.reconciliation import reconcile_prices

LOG_FILENAME = 'Reconciliation.log'
APP_NAME = "Reconciliation Analysis Pipeline"

def setup_logging(base_dir):
    """Configure logging to file + console at project level."""
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)  # ensure logs dir exists
    log_file = os.path.join(logs_dir, LOG_FILENAME)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout)  # logs also to console
        ]
    )
    return logging.getLogger(APP_NAME)

def main():
    base_dir = os.path.dirname(os.path.dirname(__file__))
    conf_path = os.path.join(base_dir, "config", "config.conf")
    config = read_config(conf_path)

    db_name = config["DATABASE_NAME"]
    output_dir = os.path.join(base_dir, sys.argv[1])
    os.makedirs(output_dir, exist_ok=True)

    logger = setup_logging(base_dir)

    logger.info(f"Starting {APP_NAME} with arguments: {sys.argv}")

    rec_file = os.path.join(output_dir, "reconciliation.csv")
    rec_sql = os.path.join(base_dir, "sql", config["RECONCILIATION_SQL"])

    conn = get_connection(db_name)
    logger.info("Application Initialized: " + APP_NAME)
    try:
        reconcile_prices(conn, rec_file, rec_sql)
        logger.info("Reconciliation complete "+rec_file)
        print("Reconciliation complete "+rec_file)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
