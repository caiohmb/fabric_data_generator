"""
Fabric Warehouse Data Generator
Generates and inserts synthetic data into Microsoft Fabric Warehouse
"""

import os
import sys
import time
import random
from datetime import datetime, timedelta
from typing import List, Tuple
import pyodbc
from faker import Faker
from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ClientSecretCredential
from dotenv import load_dotenv

try:
    from colorama import init, Fore, Style
    init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False

# Load environment variables
load_dotenv()

# Configuration
FABRIC_SERVER = os.getenv('FABRIC_SERVER')
FABRIC_DATABASE = os.getenv('FABRIC_DATABASE')
AUTH_METHOD = os.getenv('AUTH_METHOD', 'CLI')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))  # Rows to generate per cycle
BATCH_INTERVAL = int(os.getenv('BATCH_INTERVAL', 5))  # Seconds between cycles
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1000))  # Rows per SQL INSERT statement

# Initialize Faker
fake = Faker()

# Global counters for IDs
customer_id_counter = 1
order_id_counter = 1
payment_id_counter = 1


def print_colored(message: str, color: str = 'white'):
    """Print colored messages if colorama is available"""
    if HAS_COLOR:
        colors = {
            'green': Fore.GREEN,
            'yellow': Fore.YELLOW,
            'red': Fore.RED,
            'blue': Fore.BLUE,
            'cyan': Fore.CYAN,
            'white': Fore.WHITE
        }
        print(f"{colors.get(color, Fore.WHITE)}{message}{Style.RESET_ALL}")
    else:
        print(message)


def get_connection_string() -> str:
    """Build connection string for Fabric Warehouse"""
    if not FABRIC_SERVER or not FABRIC_DATABASE:
        raise ValueError("FABRIC_SERVER and FABRIC_DATABASE must be set in .env file")

    connection_string = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={FABRIC_SERVER},1433;"
        f"Database={FABRIC_DATABASE};"
        f"Encrypt=Yes;"
        f"TrustServerCertificate=No;"
    )

    return connection_string


def get_access_token() -> str:
    """Get Azure access token based on authentication method"""
    scope = "https://database.windows.net/.default"

    try:
        if AUTH_METHOD == "CLI":
            print_colored("Using Azure CLI authentication...", "cyan")
            credential = AzureCliCredential()
        elif AUTH_METHOD == "INTERACTIVE":
            print_colored("Using Interactive Browser authentication...", "cyan")
            credential = InteractiveBrowserCredential()
        elif AUTH_METHOD == "SERVICE_PRINCIPAL":
            print_colored("Using Service Principal authentication...", "cyan")
            client_id = os.getenv('AZURE_CLIENT_ID')
            client_secret = os.getenv('AZURE_CLIENT_SECRET')
            tenant_id = os.getenv('AZURE_TENANT_ID')

            if not all([client_id, client_secret, tenant_id]):
                raise ValueError("Service Principal credentials missing in .env file")

            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            raise ValueError(f"Invalid AUTH_METHOD: {AUTH_METHOD}")

        token = credential.get_token(scope)
        return token.token
    except Exception as e:
        print_colored(f"Authentication error: {str(e)}", "red")
        raise


def create_connection():
    """Create database connection to Fabric Warehouse"""
    try:
        connection_string = get_connection_string()
        access_token = get_access_token()

        # Convert token to bytes as required by pyodbc
        token_bytes = access_token.encode('utf-16-le')
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

        # SQL_COPT_SS_ACCESS_TOKEN = 1256
        conn = pyodbc.connect(connection_string, attrs_before={1256: token_struct})

        print_colored("✓ Successfully connected to Fabric Warehouse", "green")
        return conn
    except Exception as e:
        print_colored(f"✗ Connection error: {str(e)}", "red")
        raise


def create_tables_if_not_exist(conn):
    """Create tables if they don't exist"""
    cursor = conn.cursor()

    try:
        # Create customers table (Fabric Warehouse has limited data type support)
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers')
        BEGIN
            CREATE TABLE customers (
                ID INT NOT NULL,
                FIRST_NAME VARCHAR(8000),
                LAST_NAME VARCHAR(8000)
            )
        END
        """)

        # Create orders table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders')
        BEGIN
            CREATE TABLE orders (
                ID INT NOT NULL,
                USER_ID INT,
                ORDER_DATE DATETIME2(6),
                STATUS VARCHAR(8000)
            )
        END
        """)

        # Create payments table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'payments')
        BEGIN
            CREATE TABLE payments (
                ID INT NOT NULL,
                ORDERID INT,
                PAYMENTMETHOD VARCHAR(8000),
                STATUS VARCHAR(8000),
                AMOUNT DECIMAL(18, 2),
                CREATED DATETIME2(6)
            )
        END
        """)

        conn.commit()
        print_colored("✓ Tables verified/created successfully", "green")
    except Exception as e:
        print_colored(f"✗ Error creating tables: {str(e)}", "red")
        raise
    finally:
        cursor.close()


def generate_customers(count: int) -> List[Tuple]:
    """Generate fake customer data"""
    global customer_id_counter
    customers = []

    for _ in range(count):
        customer = (
            customer_id_counter,
            fake.first_name(),
            fake.last_name()
        )
        customers.append(customer)
        customer_id_counter += 1

    return customers


def generate_orders(count: int, max_customer_id: int) -> List[Tuple]:
    """Generate fake order data"""
    global order_id_counter
    orders = []
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']

    for _ in range(count):
        order = (
            order_id_counter,
            random.randint(1, max_customer_id),
            fake.date_time_between(start_date='-30d', end_date='now'),
            random.choice(statuses)
        )
        orders.append(order)
        order_id_counter += 1

    return orders


def generate_payments(count: int, max_order_id: int) -> List[Tuple]:
    """Generate fake payment data"""
    global payment_id_counter
    payments = []
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'pix']
    statuses = ['pending', 'completed', 'failed', 'refunded']

    for _ in range(count):
        payment = (
            payment_id_counter,
            random.randint(1, max_order_id),
            random.choice(payment_methods),
            random.choice(statuses),
            round(random.uniform(10.0, 5000.0), 2),
            fake.date_time_between(start_date='-30d', end_date='now')
        )
        payments.append(payment)
        payment_id_counter += 1

    return payments


def batch_insert_customers(conn, customers: List[Tuple]):
    """Insert customers using bulk insert via VALUES"""
    cursor = conn.cursor()
    chunk_size = CHUNK_SIZE  # Use configurable chunk size
    try:
        total = len(customers)
        for i in range(0, total, chunk_size):
            chunk = customers[i:i + chunk_size]

            # Build bulk INSERT statement
            values = ", ".join([f"({c[0]}, '{c[1]}', '{c[2]}')" for c in chunk])
            sql = f"INSERT INTO customers (ID, FIRST_NAME, LAST_NAME) VALUES {values}"

            cursor.execute(sql)

            if (i + chunk_size) % 5000 == 0:
                print_colored(f"    → {min(i + chunk_size, total):,}/{total:,} customers...", "white")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print_colored(f"    ✗ Error inserting customers: {str(e)}", "red")
        raise
    finally:
        cursor.close()


def batch_insert_orders(conn, orders: List[Tuple]):
    """Insert orders using bulk insert via VALUES"""
    cursor = conn.cursor()
    chunk_size = CHUNK_SIZE  # Use configurable chunk size
    try:
        total = len(orders)
        for i in range(0, total, chunk_size):
            chunk = orders[i:i + chunk_size]

            # Build bulk INSERT statement
            values = ", ".join([
                f"({o[0]}, {o[1]}, '{o[2].strftime('%Y-%m-%d %H:%M:%S')}', '{o[3]}')"
                for o in chunk
            ])
            sql = f"INSERT INTO orders (ID, USER_ID, ORDER_DATE, STATUS) VALUES {values}"

            cursor.execute(sql)

            if (i + chunk_size) % 5000 == 0:
                print_colored(f"    → {min(i + chunk_size, total):,}/{total:,} orders...", "white")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print_colored(f"    ✗ Error inserting orders: {str(e)}", "red")
        raise
    finally:
        cursor.close()


def batch_insert_payments(conn, payments: List[Tuple]):
    """Insert payments using bulk insert via VALUES"""
    cursor = conn.cursor()
    chunk_size = CHUNK_SIZE  # Use configurable chunk size
    try:
        total = len(payments)
        for i in range(0, total, chunk_size):
            chunk = payments[i:i + chunk_size]

            # Build bulk INSERT statement
            values = ", ".join([
                f"({p[0]}, {p[1]}, '{p[2]}', '{p[3]}', {p[4]}, '{p[5].strftime('%Y-%m-%d %H:%M:%S')}')"
                for p in chunk
            ])
            sql = f"INSERT INTO payments (ID, ORDERID, PAYMENTMETHOD, STATUS, AMOUNT, CREATED) VALUES {values}"

            cursor.execute(sql)

            if (i + chunk_size) % 5000 == 0:
                print_colored(f"    → {min(i + chunk_size, total):,}/{total:,} payments...", "white")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print_colored(f"    ✗ Error inserting payments: {str(e)}", "red")
        raise
    finally:
        cursor.close()


def run_generator():
    """Main generator loop"""
    print_colored("=" * 60, "cyan")
    print_colored("Fabric Warehouse Data Generator", "cyan")
    print_colored("=" * 60, "cyan")
    print_colored(f"Batch Size: {BATCH_SIZE:,} rows per table", "yellow")
    print_colored(f"Interval: {BATCH_INTERVAL} seconds", "yellow")
    print_colored("=" * 60, "cyan")
    print()

    # Connect to database
    conn = create_connection()

    # Create tables if needed
    create_tables_if_not_exist(conn)

    print()
    print_colored("Starting data generation... (Press Ctrl+C to stop)", "green")
    print()

    batch_number = 0

    try:
        while True:
            batch_number += 1
            start_time = time.time()

            print_colored(f"[Batch #{batch_number}] Starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "blue")

            # Generate data
            print_colored(f"  → Generating {BATCH_SIZE:,} customers...", "white")
            customers = generate_customers(BATCH_SIZE)

            print_colored(f"  → Generating {BATCH_SIZE:,} orders...", "white")
            orders = generate_orders(BATCH_SIZE, customer_id_counter - 1)

            print_colored(f"  → Generating {BATCH_SIZE:,} payments...", "white")
            payments = generate_payments(BATCH_SIZE, order_id_counter - 1)

            # Insert data
            print_colored(f"  → Inserting customers...", "white")
            batch_insert_customers(conn, customers)

            print_colored(f"  → Inserting orders...", "white")
            batch_insert_orders(conn, orders)

            print_colored(f"  → Inserting payments...", "white")
            batch_insert_payments(conn, payments)

            elapsed_time = time.time() - start_time
            rows_per_second = (BATCH_SIZE * 3) / elapsed_time

            print_colored(
                f"  ✓ Batch #{batch_number} completed in {elapsed_time:.2f}s "
                f"({rows_per_second:,.0f} rows/sec)",
                "green"
            )
            print_colored(
                f"  Total inserted: {(customer_id_counter-1):,} customers, "
                f"{(order_id_counter-1):,} orders, {(payment_id_counter-1):,} payments",
                "cyan"
            )
            print()

            # Wait before next batch
            time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        print()
        print_colored("=" * 60, "yellow")
        print_colored("Generator stopped by user", "yellow")
        print_colored(f"Total batches processed: {batch_number}", "yellow")
        print_colored(
            f"Final counts: {(customer_id_counter-1):,} customers, "
            f"{(order_id_counter-1):,} orders, {(payment_id_counter-1):,} payments",
            "yellow"
        )
        print_colored("=" * 60, "yellow")
    except Exception as e:
        print_colored(f"✗ Error: {str(e)}", "red")
        raise
    finally:
        conn.close()
        print_colored("Connection closed", "cyan")


if __name__ == "__main__":
    import struct  # Required for token conversion

    try:
        run_generator()
    except Exception as e:
        print_colored(f"Fatal error: {str(e)}", "red")
        sys.exit(1)
