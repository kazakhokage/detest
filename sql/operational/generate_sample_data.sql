-- Insert 1000 customers
INSERT INTO customers (name, country)
SELECT 
    'Customer ' || generate_series,
    CASE (random() * 10)::int
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'Canada'
        WHEN 2 THEN 'UK'
        WHEN 3 THEN 'Germany'
        WHEN 4 THEN 'France'
        WHEN 5 THEN 'Japan'
        WHEN 6 THEN 'Australia'
        WHEN 7 THEN 'Brazil'
        WHEN 8 THEN 'India'
        ELSE 'China'
    END
FROM generate_series(1, 1000);

-- Insert 100 products
INSERT INTO products (name, group_name)
SELECT 
    'Product ' || generate_series,
    CASE (random() * 9)::int
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        WHEN 4 THEN 'Toys'
        WHEN 5 THEN 'Sports'
        WHEN 6 THEN 'Home'
        WHEN 7 THEN 'Beauty'
        WHEN 8 THEN 'Automotive'
        ELSE 'Garden'
    END
FROM generate_series(1, 100);

-- Insert 10000 sales
INSERT INTO sales (customer_id, product_id, qty, transaction_date)
SELECT 
    (random() * 999 + 1)::int,
    (random() * 99 + 1)::int,
    (random() * 9 + 1)::int,
    CURRENT_TIMESTAMP - (random() * 30 || ' days')::interval
FROM generate_series(1, 10000);