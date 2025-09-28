#!/usr/bin/env python3
"""
Database setup script for Cardinal query optimization project
Creates test database with sample data for experimentation
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from config import DB_CONFIG


def create_database():
    """Create the experimental database"""
    # Connect to default postgres database
    conn_params = DB_CONFIG.copy()
    conn_params["database"] = "postgres"  # Connect to default database first

    conn = psycopg2.connect(**conn_params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Drop and recreate database
    try:
        db_name = DB_CONFIG["database"]
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()
        conn.close()


def setup_sample_tables():
    """Create sample tables with realistic data for testing"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create sample tables
    schema_sql = """
    -- Customers table
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        city VARCHAR(50),
        country VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Orders table
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(10,2),
        status VARCHAR(20)
    );
    
    -- Order items table
    CREATE TABLE IF NOT EXISTS order_items (
        item_id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(order_id),
        product_name VARCHAR(100),
        quantity INTEGER,
        price DECIMAL(8,2)
    );
    
    -- Create indexes for testing
    CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id);
    """

    cursor.execute(schema_sql)

    # Insert sample data
    sample_data_sql = """
    -- Insert sample customers
    INSERT INTO customers (name, email, city, country) VALUES
    ('John Doe', 'john@email.com', 'New York', 'USA'),
    ('Jane Smith', 'jane@email.com', 'London', 'UK'),
    ('Bob Johnson', 'bob@email.com', 'Toronto', 'Canada');
    
    -- Insert sample orders
    INSERT INTO orders (customer_id, total_amount, status) VALUES
    (1, 150.00, 'completed'),
    (2, 200.00, 'pending'),
    (1, 75.50, 'completed'),
    (3, 300.00, 'shipped');
    
    -- Insert sample order items
    INSERT INTO order_items (order_id, product_name, quantity, price) VALUES
    (1, 'Widget A', 2, 25.00),
    (1, 'Widget B', 1, 100.00),
    (2, 'Widget C', 1, 200.00),
    (3, 'Widget A', 3, 25.00),
    (4, 'Widget D', 1, 300.00);
    """

    cursor.execute(sample_data_sql)
    conn.commit()
    print("Sample tables and data created successfully")

    cursor.close()
    conn.close()


def main():
    """Main function to set up database and tables"""
    print(f"Setting up database with configuration:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  User: {DB_CONFIG['user']}")
    print(f"  Port: {DB_CONFIG['port']}")
    print()

    create_database()
    setup_sample_tables()

    print("\nDatabase setup complete!")
    print("You can now run the query executor scripts.")


if __name__ == "__main__":
    main()
