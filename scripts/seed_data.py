#!/usr/bin/env python3
"""
Data Seeding Script for Operational Database
Generates realistic sample data for customers, products, and sales
"""

import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import sys

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'operational',
    'user': 'postgres',
    'password': 'postgres'
}

# Data generation parameters
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 100
NUM_SALES = 10000
DAYS_BACK = 30

# Product groups for categorization
PRODUCT_GROUPS = [
    'Electronics', 'Clothing', 'Food & Beverage', 'Books & Media',
    'Toys & Games', 'Sports & Outdoors', 'Home & Garden', 'Beauty & Personal Care',
    'Automotive', 'Health & Wellness'
]

def connect_to_db():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Connected to database: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        sys.exit(1)

def generate_customers(conn, num_customers):
    """Generate customer records"""
    fake = Faker()
    cursor = conn.cursor()
    
    print(f"\n📊 Generating {num_customers} customers...")
    
    customers = []
    for i in range(num_customers):
        name = fake.name()
        country = fake.country()
        customers.append((name, country))
    
    # Batch insert for performance
    cursor.executemany(
        "INSERT INTO customers (name, country) VALUES (%s, %s)",
        customers
    )
    conn.commit()
    
    print(f"✓ Inserted {cursor.rowcount} customers")
    cursor.close()
    return cursor.rowcount

def generate_products(conn, num_products):
    """Generate product records"""
    fake = Faker()
    cursor = conn.cursor()
    
    print(f"\n📦 Generating {num_products} products...")
    
    products = []
    for i in range(num_products):
        # Generate product name (adjective + noun)
        name = f"{fake.word().capitalize()} {fake.word().capitalize()}"
        group = random.choice(PRODUCT_GROUPS)
        products.append((name, group))
    
    # Batch insert for performance
    cursor.executemany(
        "INSERT INTO products (name, group_name) VALUES (%s, %s)",
        products
    )
    conn.commit()
    
    print(f"✓ Inserted {cursor.rowcount} products")
    cursor.close()
    return cursor.rowcount

def generate_sales(conn, num_sales, days_back):
    """Generate sales transaction records"""
    cursor = conn.cursor()
    
    print(f"\n🛒 Generating {num_sales} sales transactions...")
    
    # Get customer and product IDs for foreign key references
    cursor.execute("SELECT id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM products")
    product_ids = [row[0] for row in cursor.fetchall()]
    
    if not customer_ids or not product_ids:
        print("✗ Error: No customers or products found. Generate them first.")
        return 0
    
    # Generate sales data
    sales = []
    start_date = datetime.now() - timedelta(days=days_back)
    
    for i in range(num_sales):
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        qty = random.randint(1, 10)
        
        # Random timestamp within the date range
        random_days = random.uniform(0, days_back)
        random_hours = random.uniform(0, 24)
        transaction_date = start_date + timedelta(
            days=random_days,
            hours=random_hours
        )
        
        sales.append((customer_id, product_id, qty, transaction_date))
        
        # Progress indicator
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1}/{num_sales} transactions...")
    
    # Batch insert for performance
    cursor.executemany(
        """
        INSERT INTO sales (customer_id, product_id, qty, transaction_date)
        VALUES (%s, %s, %s, %s)
        """,
        sales
    )
    conn.commit()
    
    print(f"✓ Inserted {cursor.rowcount} sales transactions")
    cursor.close()
    return cursor.rowcount

def verify_data(conn):
    """Verify inserted data"""
    cursor = conn.cursor()
    
    print("\n📈 Verification Summary:")
    print("=" * 50)
    
    # Customer count
    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]
    print(f"Customers:  {customer_count:,}")
    
    # Product count
    cursor.execute("SELECT COUNT(*) FROM products")
    product_count = cursor.fetchone()[0]
    print(f"Products:   {product_count:,}")
    
    # Sales count
    cursor.execute("SELECT COUNT(*) FROM sales")
    sales_count = cursor.fetchone()[0]
    print(f"Sales:      {sales_count:,}")
    
    # Date range
    cursor.execute("""
        SELECT 
            MIN(transaction_date)::DATE as earliest,
            MAX(transaction_date)::DATE as latest
        FROM sales
    """)
    earliest, latest = cursor.fetchone()
    print(f"\nDate Range: {earliest} to {latest}")
    
    # Sample sales by product group
    cursor.execute("""
        SELECT 
            p.group_name,
            COUNT(*) as sales_count,
            SUM(s.qty) as total_quantity
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.group_name
        ORDER BY sales_count DESC
        LIMIT 5
    """)
    
    print("\n📊 Top 5 Product Groups by Sales:")
    print("-" * 50)
    for row in cursor.fetchall():
        print(f"  {row[0]:<25} {row[1]:>6} sales  ({row[2]:>7} qty)")
    
    cursor.close()

def main():
    """Main execution function"""
    print("=" * 60)
    print("   OPERATIONAL DATABASE DATA SEEDING")
    print("=" * 60)
    
    # Connect to database
    conn = connect_to_db()
    
    try:
        # Check if data already exists
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        existing_customers = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM sales")
        existing_sales = cursor.fetchone()[0]
        cursor.close()
        
        if existing_customers > 0 or existing_sales > 0:
            print(f"\n⚠️  Warning: Database already contains data:")
            print(f"   Customers: {existing_customers:,}")
            print(f"   Sales: {existing_sales:,}")
            response = input("\n   Continue and add more data? (yes/no): ")
            if response.lower() != 'yes':
                print("   Aborted.")
                return
        
        # Generate data
        generate_customers(conn, NUM_CUSTOMERS)
        generate_products(conn, NUM_PRODUCTS)
        generate_sales(conn, NUM_SALES, DAYS_BACK)
        
        # Verify
        verify_data(conn)
        
        print("\n" + "=" * 60)
        print("✓ Data seeding completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during data generation: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
