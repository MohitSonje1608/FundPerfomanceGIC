# FundPerfomanceGIC
No problem — I can’t directly open that GitHub link, but I can still generate a professional **README.md** for your `FundPerformanceGIC` project based on what we’ve discussed earlier (data ingestion, fund performance, reconciliation, logging, configuration, etc.).



#  Fund Performance GIC

FundPerformanceGIC is a modular, scalable data pipeline for fund performance analysis and reconciliation.
It automates CSV ingestion, database loading, reconciliation of fund prices with reference data, and generates analytics outputs (e.g., best performing funds).

---

##  Project Structure

```
FundPerformanceGIC/
├── config/
│   └── config.conf                  # Configuration for database, paths, SQL references
├── data/
│   ├── external_funds/                       # Raw CSV fund reports
│   └── output/                      # Processed outputs (e.g., best performing funds)
├── jobs/
│   ├── load_all.py                  # Main data ingestion job
│   ├── fund_performance.py          # Best performing funds analysis
│   ├── reconciliation.py            # Fund vs. reference data reconciliation
├── logs/                            # Centralized logs for all modules
├── sql/
│   ├── financial_data.sql    # DDL scripts
│   ├── performance_query.sql        # Query for top performing funds
│   └── reconciliation_query.sql     # Query for price reconciliation
├── src/
│   ├── main.py                      # Config reader & entry utilities
│   ├── data_loader.py               # CSV ingestion logic
│   ├── db_utils.py                  # SQLite helper functions
│   ├── reconcilliation.py           # For Executing price reconciliation
│   └── fund_performance.py         # Business logic for performance calculations
└── tests/                           # Unit tests for key modules
```

---

##  Features

 **Automated Data Ingestion**

* Reads multiple CSV fund reports dynamically.
* Extracts `Fund_Name` and `Month` intelligently from filenames.
* Normalizes columns and loads into SQLite database.

 **Dynamic Reconciliation**

* Compares `financial_data` (fund holdings) vs. latest `bond_prices` and `equity_prices`.
* Ensures both `SEDOL` and `ISIN` are properly reconciled.

 **Performance Analytics**

* Identifies best-performing funds based on custom SQL logic.
* Generates consolidated CSV reports for performance metrics.

 **Scalable Design**

* Centralized configuration and modular scripts.

 **Logging**

* Each job writes logs to the `logs/` directory for traceability.
* Both console and file logging supported.

---

##  Configuration (`config/config.conf`)

Example:

```ini
[DATABASE]
DATABASE_NAME = assets.db

[PATHS]
MASTER_REFERENCE = master-reference-sql.sql.sql
FUNDS_TABLE_DDL = financial_data.sql
RECONCILIATION_SQL = reconciliation.sql
PERFORMANCE_SQL = performance_sql.sql

[TABLES]
TABLE_NAME = financial_data
```

---

##  Running the Pipeline

### 1️⃣ Load Data

```bash
python jobs/load_all.py data/input/
```

### 2️⃣ Generate Best Performing Funds

```bash
python jobs/fund_performance.py data/output/
```

### 3️⃣ Run Reconciliation

```bash
python jobs/reconciliation.py data/output/
```

All logs will be available in the `logs/` folder.

---

##  Technical Highlights

* **Language:** Python 3.x
* **Database:** SQLite
* **Logging:** Standard Python `logging` (rotating file handlers supported)
* **Modular Design:** Extensible for additional asset classes
* **Testing:** Supports unit tests under `/tests`

---

##  Future Enhancements

* Migrate to PostgreSQL for scalability
* Introduce ETL scheduling via Airflow
* Add data validation and audit checks
* Automate reconciliation output visualization (e.g., dashboards)

---


Mohit Sonje
🔗 [GitHub: MohitSonje1608](https://github.com/MohitSonje1608)



