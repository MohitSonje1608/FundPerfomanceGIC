# jobs/load_all.py
import os
import sys
import logging
from src.main import read_config
from src.db_utils import get_connection, execute_sql_file
from src.data_loader import load_csv_data

APP_NAME = "Data Loader Pipeline: Ingest"
LOG_FILENAME = "ingest.log"

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
    base_dir = os.path.dirname(os.path.dirname(__file__))  # project root
    conf_path = os.path.join(base_dir, "config", "config.conf")

    logger = setup_logging(base_dir)
    logger.info("Starting %s with arguments: %s", APP_NAME, sys.argv)

    config = read_config(conf_path)
    db_name = config["DATABASE_NAME"]
    ddl_file = os.path.join(base_dir, "sql", config["MASTER_REFERENCE"])
    funds_ddl = os.path.join(base_dir, "sql", config["FUNDS_TABLE_DDL"])
    table_name = config["TABLE_NAME"]
    csv_dir = sys.argv[1]

    conn = get_connection(db_name)
    logger.info("Application Initialized: %s", APP_NAME)

    try:
        execute_sql_file(conn, ddl_file)
        execute_sql_file(conn, funds_ddl)
        logger.info("Tables created successfully")

        load_csv_data(conn, csv_dir, table_name)
        logger.info("CSV Data loaded successfully into the table")

        logger.info("Application finished: %s", APP_NAME)
    except Exception as e:
        logger.exception("Pipeline failed due to error: %s", e)
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()
