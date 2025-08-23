-- 1. Customers
CREATE TABLE bronze."Customers" (
    "Customer_id" SERIAL PRIMARY KEY,
    "First_Name" VARCHAR(50),
    "Last_Name" VARCHAR(50),
    "Email" VARCHAR(100),
    "Phone_number" VARCHAR(15),
    "City" VARCHAR(50),
    "Signup_date" DATE
);

-- 2. Restaurants
CREATE TABLE bronze."Restaurants" (
    "Restaurant_id" SERIAL PRIMARY KEY,
    "Name" VARCHAR(100),
    "Cuisine_type" VARCHAR(50),
    "City" VARCHAR(50),
    "Rating" NUMERIC(3,2),
    "Open_date" DATE
);

-- 3. Delivery_Partners
CREATE TABLE bronze."Delivery_Partners" (
    "Partner_id" SERIAL PRIMARY KEY,
    "Partner_name" VARCHAR(100),
    "Phone_number" VARCHAR(15),
    "City" VARCHAR(50),
    "Vehicle_type" VARCHAR(50),
    "Rating" NUMERIC(3,2),
    "Join_date" DATE
);

-- 4. Orders
CREATE TABLE bronze."Orders" (
    "Order_id" SERIAL PRIMARY KEY,
    "Customer_id" INT REFERENCES bronze."Customers"("Customer_id"),
    "Restaurant_id" INT REFERENCES bronze."Restaurants"("Restaurant_id"),
    "Order_date" DATE,
    "Order_amount" NUMERIC(10,2),
    "Payment_mode" VARCHAR(50),
    "Delivery_status" VARCHAR(20)  -- 'Delivered' / 'Cancelled'
);

-- 5. Order_Items
CREATE TABLE bronze."Order_Items" (
    "Order_item_id" SERIAL PRIMARY KEY,
    "Order_id" INT REFERENCES bronze."Orders"("Order_id"),
    "Menu_item" VARCHAR(100),
    "Quantity" INT,
    "Price" NUMERIC(10,2)
);
