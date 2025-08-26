import psycopg2
import json
import logging
from datetime import datetime

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
def get_connection():
    return psycopg2.connect(
        dbname="mydb",   # change to your db
        user="postgres",
        password="Kalam5017",
        host="localhost",
        port="5432"
    )

# -----------------------------
# Create schema if not exists
# -----------------------------
def create_schema():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
            cur.execute("CREATE SCHEMA IF NOT EXISTS audit;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS audit.rejected_rows (
                    table_name TEXT,
                    reason TEXT,
                    row_data JSONB,
                    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.commit()
    logging.info("Schemas and audit table ready.")

# -----------------------------
# Helper: Insert rejected rows
# -----------------------------
def log_rejections(table_name, reason, rows, cur):
    for row in rows:
        cur.execute(
            "INSERT INTO audit.rejected_rows (table_name, reason, row_data) VALUES (%s, %s, %s)",
            (table_name, reason, json.dumps(row))
        )

# -----------------------------
# Generic Silver Loader
# -----------------------------
def load_silver_table(table_name, select_sql, dq_checks, pk_column):
    """
    table_name: str -> silver table
    select_sql: str -> transformation SQL
    dq_checks: list of (condition, reason)
    pk_column: primary key for dedup
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            silver_table = f'silver."{table_name}"'

            # 1. Drop & recreate table
            cur.execute(f"DROP TABLE IF EXISTS {silver_table};")
            cur.execute(f"CREATE TABLE {silver_table} AS {select_sql} WITH NO DATA;")
            cur.execute(f"INSERT INTO {silver_table} {select_sql}")

            # 2. Apply DQ checks
            for condition, reason in dq_checks:
                dq_sql = f"""
                    WITH bad_rows AS (
                        SELECT * FROM {silver_table} WHERE {condition}
                    )
                    DELETE FROM {silver_table} s
                    USING bad_rows b
                    WHERE s."{pk_column}" = b."{pk_column}"
                    RETURNING row_to_json(b.*);
                """
                cur.execute(dq_sql)
                bad_rows = [row[0] for row in cur.fetchall()]
                log_rejections(table_name, reason, bad_rows, cur)

            # 3. Deduplicate based on PK
            dedup_sql = f"""
                DELETE FROM {silver_table} a
                USING (
                    SELECT "{pk_column}", MIN(ctid) AS keep_ctid
                    FROM {silver_table}
                    GROUP BY "{pk_column}"
                    HAVING COUNT(*) > 1
                ) dup
                WHERE a."{pk_column}" = dup."{pk_column}" AND a.ctid <> dup.keep_ctid;
            """
            cur.execute(dedup_sql)
        conn.commit()
    logging.info(f"Silver table {table_name} built successfully.")

# -----------------------------
# Table Configurations
# -----------------------------
def build_silver():
    # Customers
    load_silver_table(
        "customers",
        """
        SELECT DISTINCT
            "Customer_id",
            UPPER(TRIM("First_Name")) AS "first_name",
            UPPER(TRIM("Last_Name")) AS "last_name",
            LOWER(TRIM("Email")) AS "email",
            TRIM("Phone_number") AS "phone_number",
            INITCAP(TRIM("City")) AS "city",
            "Signup_date"
        FROM bronze."Customers"
        """,
        dq_checks=[
            ('"Customer_id" IS NULL', 'Missing Customer ID'),
            ('"email" NOT LIKE \'%@%\'', 'Invalid Email Format'),
            ('"Signup_date" IS NULL', 'Missing Signup Date')
        ],
        pk_column="Customer_id"
    )

    # Restaurants
    load_silver_table(
        "restaurants",
        """
        SELECT DISTINCT
            "Restaurant_id",
            INITCAP(TRIM("Name")) AS "restaurant_name",
            INITCAP(TRIM("Cuisine_type")) AS "cuisine_type",
            INITCAP(TRIM("City")) AS "city",
            "Rating",
            "Open_date"
        FROM bronze."Restaurants"
        """,
        dq_checks=[
            ('"Restaurant_id" IS NULL', 'Missing Restaurant ID'),
            ('"Open_date" IS NULL', 'Missing Open Date')
        ],
        pk_column="Restaurant_id"
    )

    # Orders
    load_silver_table(
        "orders",
        """
        SELECT DISTINCT
            "Order_id",
            "Customer_id",
            "Customer_City",
            "Restaurant_id",
            "Partner_id",
            "Order_date",
            "Delivery_status",
            "Payment_mode",
            "Order_amount"
        FROM bronze."Orders"
        """,
        dq_checks=[
            ('"Order_id" IS NULL', 'Missing Order ID'),
            ('"Order_amount" < 0', 'Negative Amount')
        ],
        pk_column="Order_id"
    )

    # Order Items
    load_silver_table(
        "order_items",
        """
        SELECT DISTINCT
            "Order_item_id",
            "Order_id",
            "Menu_item",
            "Quantity",
            "Price"
        FROM bronze."Order_Items"
        """,
        dq_checks=[
            ('"Order_item_id" IS NULL', 'Missing Order Item ID'),
            ('"Quantity" <= 0', 'Invalid Quantity'),
            ('"Price" < 0', 'Negative Price')
        ],
        pk_column="Order_item_id"
    )

    # Delivery Partners
    load_silver_table(
        "delivery_partners",
        """
        SELECT DISTINCT
            "Partner_id",
            "Partner_name",
            TRIM("Phone_number") AS "phone_number",
            INITCAP(TRIM("City")) AS "city",
            "Vehicle_type",
            "Rating",
            "Join_date"
        FROM bronze."Delivery_Partners"
        """,
        dq_checks=[
            ('"Partner_id" IS NULL', 'Missing Partner ID')
        ],
        pk_column="Partner_id"
    )

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    create_schema()
    build_silver()
    logging.info("Silver layer build complete.")
