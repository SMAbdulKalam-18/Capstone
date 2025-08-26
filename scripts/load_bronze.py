import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

# -------------------------
# Config: Paths & DB creds
# -------------------------
BRONZE_INPUTS = '/home/nineleaps/PycharmProjects/Capstone/bronze_inputs'
LOGS_FOLDER = '/home/nineleaps/PycharmProjects/Capstone/logs'

DB_CONFIG = {
    'host': 'localhost',
    'database': 'mydb',
    'user': 'postgres',
    'password': 'Kalam5017'
}

os.makedirs(LOGS_FOLDER, exist_ok=True)
log_file = os.path.join(LOGS_FOLDER, 'load_log.txt')

# -------------------------
# Case-sensitive table/column mapping
# -------------------------
tables = {
    "Customers": ["Customer_id", "First_Name", "Last_Name", "Email", "Phone_number", "City", "Signup_date"],
    "Restaurants": ["Restaurant_id", "Name", "Cuisine_type", "City", "Rating", "Open_date"],
    "Delivery_Partners": ["Partner_id", "Partner_name", "Phone_number", "City", "Vehicle_type", "Rating", "Join_date"],
    "Orders": ["Order_id", "Customer_id","Customer_City", "Restaurant_id","Partner_id", "Order_date", "Delivery_status", "Payment_mode","Order_amount"],
    "Order_Items": ["Order_item_id", "Order_id", "Menu_item", "Quantity", "Price"]
}

# -------------------------
# Date columns for conversion
# -------------------------
date_columns = {
    "Customers": ["Signup_date"],
    "Restaurants": ["Open_date"],
    "Delivery_Partners": ["Join_date"],
    "Orders": ["Order_date"]
}

# -------------------------
# Connect to PostgreSQL
# -------------------------
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("Connected to PostgreSQL successfully!")
except Exception as e:
    print("Connection failed:", e)
    exit()

# -------------------------
# Load CSVs into Bronze tables
# -------------------------
with open(log_file, 'w') as log:
    for table, cols in tables.items():
        csv_path = os.path.join(BRONZE_INPUTS, f"{table}.csv")

        if not os.path.exists(csv_path):
            print(f"❌ CSV not found for table {table}: {csv_path}")
            log.write(f"{table}: CSV not found\n")
            continue

        # Read CSV
        df = pd.read_csv(csv_path)

        # Convert date columns to YYYY-MM-DD
        if table in date_columns:
            for col in date_columns[table]:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

        # Convert numeric columns to proper types (if needed)
        numeric_cols = {
            "Restaurants": ["Rating"],
            "Delivery_Partners": ["Rating"],
            "Orders": ["Order_amount"],
            "Order_Items": ["Quantity", "Price"]
        }
        if table in numeric_cols:
            for col in numeric_cols[table]:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Prepare insert statement
        insert = sql.SQL("INSERT INTO bronze.{} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, cols)),
            sql.SQL(', ').join(sql.Placeholder() * len(cols))
        )

        # Convert dataframe rows to list of tuples
        values_list = df[cols].values.tolist()

        # Execute batch insert
        execute_batch(cur, insert, values_list)
        conn.commit()

        log.write(f"{table}: {len(df)} rows loaded\n")
        print(f"{table}: {len(df)} rows loaded")

# -------------------------
# Close connection
# -------------------------
cur.close()
conn.close()
print(f"All tables loaded successfully, logs saved to {log_file}")
