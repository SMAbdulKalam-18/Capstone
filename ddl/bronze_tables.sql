-- 1. Customers
CREATE TABLE bronze."Customers" (
    "Customer_id" VARCHAR(20),
    "First_Name" VARCHAR(50),
    "Last_Name" VARCHAR(50),
    "Email" VARCHAR(100),
    "Phone_number" VARCHAR(20),
    "City" VARCHAR(50),
    "Signup_date" DATE
);

-- 2. Restaurants
CREATE TABLE bronze."Restaurants" (
    "Restaurant_id" VARCHAR(20),
    "Name" VARCHAR(100),
    "Cuisine_type" VARCHAR(50),
    "City" VARCHAR(50),
    "Rating" NUMERIC(3,2),
    "Open_date" DATE
);

-- 3. Delivery_Partners
CREATE TABLE bronze."Delivery_Partners" (
    "Partner_id" VARCHAR(20),
    "Partner_name" VARCHAR(100),
    "Phone_number" VARCHAR(20),
    "City" VARCHAR(50),
    "Vehicle_type" VARCHAR(50),
    "Rating" NUMERIC(3,2),
    "Join_date" DATE
);


-- 4. Orders
CREATE TABLE bronze."Orders" (
    "Order_id" VARCHAR(20),
    "Customer_id" VARCHAR(20),
    "Customer_City" VARCHAR(50),
    "Restaurant_id" VARCHAR(20),
    "Partner_id" VARCHAR(20),
    "Order_date" TIMESTAMP,
    "Delivery_status" VARCHAR(20),
    "Payment_mode" VARCHAR(50),
    "Order_amount" NUMERIC(10,2)
);

-- 5. Order_Items
CREATE TABLE bronze."Order_Items" (
    "Order_item_id" VARCHAR(20),
    "Order_id" VARCHAR(20),
    "Menu_item" VARCHAR(100),
    "Quantity" INT,
    "Price" NUMERIC(10,2)
);
