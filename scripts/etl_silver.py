import psycopg2
import logging
from datetime import datetime
import json

# -----------------------------
# Setup Logging
# -----------------------------
logging.basicConfig(
    filename='logs/etl_silver.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# -----------------------------
# Database Connection
# -----------------------------
conn = psycopg2.connect(
    dbname='mydb',      # replace with your DB name
    user='postgres',      # replace with your username
    password='Kalam5017',  # replace with your password
    host='localhost',      # replace if needed
    port='5432'
)
conn.autocommit = True

# -----------------------------
# SQL Transformation Files
# -----------------------------
tables_sql = {
    "Customers": "sql/silver_customers.sql",
    "Restaurants": "sql/silver_restaurants.sql",
    "Delivery_Partners": "sql/silver_delivery_partners.sql",
    "Orders": "sql/silver_orders.sql",
    "Order_Items": "sql/silver_order_items.sql"
}

# -----------------------------
# Data Quality Checks
# -----------------------------
dq_checks = {
    "Customers": [
        {"query": 'SELECT "Customer_id" FROM silver."Customers" GROUP BY "Customer_id" HAVING COUNT(*)>1', "reason": "Duplicate PK"},
        {"query": 'SELECT * FROM silver."Customers" WHERE "Email" NOT LIKE \'%@%\'', "reason": "Invalid Email"}
    ],
    "Restaurants": [
        {"query": 'SELECT "Restaurant_id" FROM silver."Restaurants" GROUP BY "Restaurant_id" HAVING COUNT(*)>1', "reason": "Duplicate PK"},
        {"query": 'SELECT * FROM silver."Restaurants" WHERE "Rating" NOT BETWEEN 1 AND 5', "reason": "Invalid Rating"}
    ],
    "Delivery_Partners": [
        {"query": 'SELECT "Partner_id" FROM silver."Delivery_Partners" GROUP BY "Partner_id" HAVING COUNT(*)>1', "reason": "Duplicate PK"},
        {"query": 'SELECT * FROM silver."Delivery_Partners" WHERE "Rating" NOT BETWEEN 1 AND 5', "reason": "Invalid Rating"}
    ],
    "Orders": [
        {"query": 'SELECT "Order_id" FROM silver."Orders" GROUP BY "Order_id" HAVING COUNT(*)>1', "reason": "Duplicate PK"},
        {"query": """
            SELECT * FROM silver."Orders"
            WHERE "Customer_id" NOT IN (SELECT "Customer_id" FROM silver."Customers")
               OR "Restaurant_id" NOT IN (SELECT "Restaurant_id" FROM silver."Restaurants")
               OR "Delivery_status" NOT IN ('Delivered','Cancelled')
               OR "Payment_mode" NOT IN ('Wallet','COD','UPI','Card')
        """, "reason": "Invalid FK or enum"}
    ],
    "Order_Items": [
        {"query": 'SELECT "Order_item_id" FROM silver."Order_Items" GROUP BY "Order_item_id" HAVING COUNT(*)>1', "reason": "Duplicate PK"},
        {"query": 'SELECT * FROM silver."Order_Items" WHERE "Quantity"<=0 OR "Price"<0', "reason": "Invalid Quantity/Price"},
        {"query": 'SELECT * FROM silver."Order_Items" WHERE "Order_id" NOT IN (SELECT "Order_id" FROM silver."Orders")', "reason": "Invalid FK"}
    ]
}

# -----------------------------
# Create Audit Table
# -----------------------------
def create_audit_table():
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS audit;
            CREATE TABLE IF NOT EXISTS audit.rejected_rows (
                table_name TEXT,
                row_data JSONB,
                reason TEXT,
                created_at TIMESTAMP DEFAULT now()
            );
        """)
    logging.info("Audit table ready.")

# -----------------------------
# Run Silver SQL Transformations
# -----------------------------
def build_silver():
    logging.info("Starting Silver layer ETL...")
    for table, sql_file in tables_sql.items():
        try:
            with open(sql_file, 'r') as f:
                query = f.read()
            with conn.cursor() as cur:
                cur.execute(query)
            logging.info(f"Transformed table {table} using {sql_file}")
        except Exception as e:
            logging.error(f"Error building table {table}: {e}")

# -----------------------------
# Run DQ Checks and Log Rejected Rows
# -----------------------------
def run_dq_checks():
    logging.info("Starting DQ checks...")
    for table, checks in dq_checks.items():
        for check in checks:
            try:
                with conn.cursor() as cur:
                    cur.execute(check["query"])
                    rows = cur.fetchall()
                    if rows:
                        for row in rows:
                            cur.execute(
                                "INSERT INTO audit.rejected_rows(table_name, row_data, reason) VALUES (%s, %s, %s)",
                                (table, json.dumps(row), check["reason"])
                            )
                        logging.warning(f"DQ Check failed on {table}: {check['reason']} ({len(rows)} rows)")
            except Exception as e:
                logging.error(f"Error in DQ check for {table}: {e}")

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    start_time = datetime.now()
    logging.info("ETL Silver process started.")

    create_audit_table()
    build_silver()
    run_dq_checks()

    end_time = datetime.now()
    logging.info(f"ETL Silver process completed. Duration: {end_time - start_time}")

    conn.close()
