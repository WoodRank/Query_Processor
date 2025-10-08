import csv
import random
from faker import Faker
import os

# --- Configuration ---
NUM_CUSTOMERS = 15000
NUM_ORDERS = 150000

# Initialize Faker for realistic-looking data
fake = Faker()

print(f"Generating {NUM_CUSTOMERS} customers...")

# --- 1. Generate customers.csv ---
customers_data = []
# A dictionary to store the country for each customer, to be used in orders.
customer_countries = {}

for customer_id in range(1, NUM_CUSTOMERS + 1):
    country = fake.country()
    customers_data.append([
        customer_id,
        fake.name(),
        country,
        str(random.choice([True, False])).lower(), # is_active as 'true' or 'false'
        round(random.uniform(50.0, 10000.0), 2)    # balance
    ])
    customer_countries[customer_id] = country

with open('customers.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['customer_id', 'name', 'country', 'is_active', 'balance'])
    writer.writerows(customers_data)

print(f"Generating {NUM_ORDERS} orders...")

# --- 2. Generate orders.csv ---
orders_data = []
for order_id in range(1, NUM_ORDERS + 1):
    # Pick a random customer for this order
    customer_id = random.randint(1, NUM_CUSTOMERS)
    
    orders_data.append([
        order_id,
        customer_id,
        random.choice(['SHIPPED', 'OPEN', 'CANCELLED']), # status
        round(random.uniform(40.0, 600.0), 2),        # total
        customer_countries[customer_id],                # country from the customer record
        fake.date_between(start_date='-3y', end_date='today').year # order_year
    ])

with open('orders.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['order_id', 'customer_id', 'status', 'total', 'country', 'order_year'])
    writer.writerows(orders_data)

print(f"\nSuccessfully created customers.csv and orders.csv in the current directory.")

