# jobs/fund_performance.py
import os
import sys
import logging
from src.main import read_config
from src.db_utils import get_connection
from src.fund_performance import best_performing_funds

LOG_FILENAME = 'fund_performance.log'
APP_NAME = "Best Performing Funds"

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
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(APP_NAME)

def main():
    base_dir = os.path.dirname(os.path.dirname(__file__))
    conf_path = os.path.join(base_dir, "config", "config.conf")
    config = read_config(conf_path)

    db_name = config["DATABASE_NAME"]
    # csv_dir = os.path.join(base_dir, config["CSV_FILE_DIR"])
    output_dir = os.path.join(base_dir, sys.argv[1])
    os.makedirs(output_dir, exist_ok=True)

    logger = setup_logging(base_dir)

    logger.info(f"Starting {APP_NAME} with arguments: {sys.argv}")

    perf_file = os.path.join(output_dir, "best_performing_funds.csv")
    perf_sql = os.path.join(base_dir, "sql", config["PERFORMANCE_SQL"])

    conn = get_connection(db_name)
    try:
        best_performing_funds(conn, perf_file, perf_sql)
        logger.info("Fund performance complete" +perf_file)
        print("Fund performance complete " +perf_file)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
