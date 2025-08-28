import psycopg2
import logging

# -----------------------------
# Logging Setup
# -----------------------------
logging.basicConfig(
    filename='etl_day3.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# -----------------------------
# Build Gold Layer
# -----------------------------
def build_gold(conn):
    cur = conn.cursor()
    try:
        logging.info("Creating Gold tables...")

        gold_queries = [
            # 1. Orders Summary
            """
            CREATE TABLE IF NOT EXISTS gold.orders_summary AS
WITH order_level AS (
    SELECT
        o."Order_id",
        o."Customer_id",
        o."Customer_City",
        o."Payment_mode",
        o."Delivery_status",
        COUNT(oi."Order_item_id") AS "items_count",
        SUM(oi."Price" * oi."Quantity") AS "order_value"
    FROM silver."orders" o
    LEFT JOIN silver."order_items" oi
           ON o."Order_id" = oi."Order_id"
    GROUP BY o."Order_id", o."Customer_id", o."Customer_City", o."Payment_mode", o."Delivery_status"
),
city_stats AS (
    -- city-level delivery rate (percent delivered per city)
    SELECT
        order_level."Customer_City" AS "Customer_City",
        ROUND(
          100.0 * SUM(CASE WHEN order_level."Delivery_status" = 'Delivered' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*),0)
        , 2) AS "city_delivery_rate"
    FROM order_level
    GROUP BY order_level."Customer_City"
),
agg AS (
    SELECT
        COUNT(DISTINCT order_level."Order_id") AS "total_orders",

        -- 1. Basket Size (avg items per order)
        ROUND(AVG(order_level."items_count")::numeric, 2) AS "avg_basket_size",

        -- 2. Payment Mode Split (percentages)
        ROUND(100.0 * SUM(CASE WHEN order_level."Payment_mode" = 'COD' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "cash_share_pct",
        ROUND(100.0 * SUM(CASE WHEN order_level."Payment_mode" = 'Card' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "card_share_pct",
        ROUND(100.0 * SUM(CASE WHEN order_level."Payment_mode" = 'UPI' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "upi_share_pct",
        ROUND(100.0 * SUM(CASE WHEN order_level."Payment_mode" = 'Wallet' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "wallet_share_pct",

        -- 3. High-Value Order Share (> ₹1000)
        ROUND(100.0 * SUM(CASE WHEN order_level."order_value" > 1000 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "high_value_order_share_pct",

        -- 4. Delivery Success Rate (overall)
        ROUND(100.0 * SUM(CASE WHEN order_level."Delivery_status" = 'Delivered' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS "delivery_success_rate_pct",

        -- 5. City-wise Delivery Reliability (average across cities)
        (SELECT ROUND(AVG(cs."city_delivery_rate")::numeric, 2) FROM city_stats cs) AS "avg_city_reliability_pct"

    FROM order_level
)
SELECT * FROM agg;
            """,

            # 2. Menu Performance
            """
            CREATE TABLE IF NOT EXISTS gold.menu_performance AS
WITH item_stats AS (
    SELECT
        oi."Menu_item",
        r."cuisine_type" AS "Cuisine",
        COUNT(DISTINCT o."Order_id") AS total_orders,
        SUM(oi."Quantity") AS total_quantity_sold,
        SUM(oi."Quantity" * oi."Price") AS total_revenue
    FROM silver."order_items" oi
    JOIN silver."orders" o
        ON oi."Order_id" = o."Order_id"
    JOIN silver."restaurants" r
        ON o."Restaurant_id" = r."Restaurant_id"
    GROUP BY oi."Menu_item", r."cuisine_type"
),
cuisine_totals AS (
    SELECT
        "Cuisine",
        SUM(total_revenue) AS cuisine_total_revenue
    FROM item_stats
    GROUP BY "Cuisine"
)
SELECT
    i."Menu_item",
    i."Cuisine",
    i.total_orders,
    i.total_quantity_sold,
    i.total_revenue,

    -- Popularity Index: % of total orders containing this item
    ROUND(
        100.0 * i.total_orders / NULLIF((SELECT COUNT(DISTINCT "Order_id") FROM silver."orders"), 0),
        2
    ) AS popularity_index,

    -- Cuisine Revenue Share: Item’s revenue as % of its cuisine revenue
    ROUND(
        100.0 * i.total_revenue / NULLIF(c.cuisine_total_revenue, 0),
        2
    ) AS cuisine_revenue_share

FROM item_stats i
JOIN cuisine_totals c
    ON i."Cuisine" = c."Cuisine";
            """,

            # 3. Customer Summary
            """
            CREATE TABLE IF NOT EXISTS gold.customer_summary AS
WITH base AS (
    SELECT
        c."Customer_id",
        c."city",
        DATE_TRUNC('month', c."Signup_date") AS acquisition_month,
        MIN(o."Order_date") AS first_order_date,
        MAX(o."Order_date") AS last_order_date,
        COUNT(o."Order_id") AS total_orders,
        EXTRACT(DAY FROM (CURRENT_DATE - MAX(o."Order_date"))) AS days_since_last_order
    FROM silver."customers" c
    LEFT JOIN silver."orders" o
        ON c."Customer_id" = o."Customer_id"
    GROUP BY c."Customer_id", c."city", acquisition_month
),
monthly AS (
    SELECT
        acquisition_month,
        "city",
        COUNT(DISTINCT "Customer_id") AS new_customers,
        ROUND(100.0 * COUNT(*) FILTER (WHERE total_orders > 1) / NULLIF(COUNT(*),0), 2) AS retention_rate_pct,
        ROUND(100.0 * COUNT(*) FILTER (WHERE days_since_last_order > 90) / NULLIF(COUNT(*),0), 2) AS dormant_customer_pct,
        COUNT(*) FILTER (WHERE days_since_last_order <= 30) AS active_customers,
        ROUND(AVG(EXTRACT(DAY FROM (last_order_date - first_order_date))), 2) AS avg_first_to_last_order_lag
    FROM base
    GROUP BY acquisition_month, "city"
)
SELECT
    acquisition_month,
    "city",
    SUM(new_customers) OVER (PARTITION BY "city" ORDER BY acquisition_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_customers,  -- cumulative
    new_customers,
    retention_rate_pct,
    dormant_customer_pct,
    active_customers,
    avg_first_to_last_order_lag
FROM monthly;
            """,

            # 4. Restaurant Summary
            """
            CREATE TABLE IF NOT EXISTS gold.restaurant_summary AS
WITH base AS (
    SELECT
        r."Restaurant_id",
        r."city",
        DATE_TRUNC('month', r."Open_date") AS opening_month,
        COUNT(DISTINCT o."Order_id") AS total_orders,
        ROUND(AVG(r."Rating"),2) AS avg_rating,
        SUM(oi."Quantity" * oi."Price") AS total_revenue
    FROM silver."restaurants" r
    LEFT JOIN silver."orders" o
        ON r."Restaurant_id" = o."Restaurant_id"
    LEFT JOIN silver."order_items" oi
        ON o."Order_id" = oi."Order_id"
    GROUP BY r."Restaurant_id", r."city", opening_month
),
monthly AS (
    SELECT
        opening_month,
        "city",
        COUNT(DISTINCT "Restaurant_id") AS new_restaurants,
        SUM(COALESCE(total_revenue,0) * COALESCE(avg_rating,0)) AS performance_score
    FROM base
    GROUP BY opening_month, "city"
),
cumulative AS (
    SELECT
        m.opening_month,
        m."city",
        m.new_restaurants,
        SUM(m.new_restaurants) OVER (
            PARTITION BY m."city"
            ORDER BY m.opening_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_restaurants,
        m.performance_score
    FROM monthly m
)
SELECT * FROM cumulative;
            """,

            # 5. Partner Summary
            """
            CREATE TABLE IF NOT EXISTS gold.partner_summary AS
WITH base AS (
    SELECT
        p."Partner_id",
        p."Vehicle_type",
        p."Join_date",
        COUNT(DISTINCT o."Order_id") AS orders_delivered,
        ROUND(AVG(p."Rating"),2) AS avg_rating
    FROM silver."delivery_partners" p
    LEFT JOIN silver."orders" o
        ON p."Partner_id" = o."Partner_id"
    GROUP BY p."Partner_id", p."Vehicle_type", p."Join_date"
),

vehicle_level AS (
    SELECT
        "Vehicle_type",
        COUNT(DISTINCT "Partner_id") AS total_partners,
        ROUND(AVG(orders_delivered),2) AS avg_orders_per_partner,
        ROUND(AVG(avg_rating),2) AS avg_partner_rating,
        ROUND(100.0 * COUNT(*) FILTER (WHERE CURRENT_DATE - "Join_date" > 180) / NULLIF(COUNT(*),0),2) AS partner_retention_rate
    FROM base
    GROUP BY "Vehicle_type"
),

overall AS (
    SELECT
        COUNT(DISTINCT "Partner_id") AS total_partners,
        ROUND(AVG(orders_delivered),2) AS avg_orders_per_partner,
        ROUND(AVG(avg_rating),2) AS avg_partner_rating_overall,
        ROUND(100.0 * COUNT(*) FILTER (WHERE CURRENT_DATE - "Join_date" > 180) / NULLIF(COUNT(*),0),2) AS partner_retention_rate_overall
    FROM base
)

SELECT * FROM vehicle_level
UNION ALL
SELECT
    'ALL' AS "Vehicle_type",
    total_partners,
    avg_orders_per_partner,
    avg_partner_rating_overall,
    partner_retention_rate_overall
FROM overall;

            """
        ]

        for query in gold_queries:
            cur.execute(query)
            conn.commit()

        logging.info("Gold layer tables created successfully!")
    except Exception as e:
        logging.error(f"Error creating Gold tables: {e}")
        conn.rollback()
    finally:
        cur.close()

# -----------------------------
# Reconciliation
# -----------------------------
def reconcile_gold(conn):
    cur = conn.cursor()
    try:
        logging.info("Starting reconciliation...")

        reconciliation_queries = {
            "total_orders": 'SELECT COUNT(*) FROM silver."orders"',
            "gold_total_orders": 'SELECT COUNT(*) FROM gold.orders_summary',
            "total_customers": 'SELECT COUNT(*) FROM silver."customers"',
            "gold_total_customers": 'SELECT COUNT(*) FROM gold.customer_summary',
            "total_restaurants": 'SELECT COUNT(*) FROM silver."restaurants"',
            "gold_total_restaurants": 'SELECT COUNT(*) FROM gold.restaurant_summary',
            "total_partners": 'SELECT COUNT(*) FROM silver."delivery_partners"',
            "gold_total_partners": 'SELECT COUNT(*) FROM gold.partner_summary'
        }

        for name, query in reconciliation_queries.items():
            cur.execute(query)
            result = cur.fetchone()[0]
            logging.info(f"{name}: {result}")
            print(f"{name}: {result}")

        logging.info("Reconciliation completed successfully!")
    except Exception as e:
        logging.error(f"Reconciliation error: {e}")
    finally:
        cur.close()

# -----------------------------
# Day 3 Pipeline Orchestration
# -----------------------------
def run_day3_pipeline():
    conn = psycopg2.connect(
        dbname='mydb',
        user='postgres',
        password='Kalam5017',
        host='localhost',
        port='5432'
    )

    print("=== Starting Day 3 ETL: Gold Layer ===")
    build_gold(conn)
    print("=== Running Reconciliation ===")
    reconcile_gold(conn)
    conn.close()
    print("=== Day 3 ETL Completed ===")

# -----------------------------
# Run if script is executed
# -----------------------------
if __name__ == "__main__":
    run_day3_pipeline()
