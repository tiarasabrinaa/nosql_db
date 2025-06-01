import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Jumlah record
NUM_CUSTOMERS = 50000
NUM_EMPLOYEES = 50000
NUM_PRODUCTS = 10000
NUM_TRANSACTIONS = 500000

# Helper untuk random choice
def random_category():
    return random.choice(['Beverages', 'Snacks', 'Household', 'Personal Care', 'Baby Products', 'Pet Supplies'])

def random_membership():
    return random.choice(['Basic', 'Silver', 'Gold', 'Platinum'])

def random_device():
    return random.choice(['Android', 'iOS', 'Web'])

def random_payment():
    return random.choice(['Credit Card', 'Debit Card', 'E-Wallet', 'Cash'])

def random_job():
    return random.choice(['Cashier', 'Store Manager', 'Inventory Staff', 'Sales Associate', 'Security'])

def random_description():
    return random.choice(['Restock', 'Sold', 'Damaged', 'Returned'])

# Generate Customers Data
customers = []
for _ in range(NUM_CUSTOMERS):
    customers.append({
        'user_id': fake.uuid4(),
        'name': fake.name(),
        'email': fake.email(),
        'address': fake.address().replace("\n", ", "),
        'city': fake.city(),
        'province': fake.state(),
        'postal_code': fake.postcode(),
        'phone_number': fake.phone_number(),
        'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=80),
        'registration_date': fake.date_between(start_date='-5y', end_date='today'),
        'last_login': fake.date_time_this_year(),
        'gender': random.choice(['M', 'F']),
        'is_active': random.choice([True, False]),
        'membership_level': random_membership(),
        'total_spent': round(random.uniform(10, 5000), 2),
        'number_of_orders': random.randint(1, 100),
        'favorite_category': random_category(),
        'device': random_device(),
        'referral_code': fake.lexify(text='?????-#####'),
        'job_title': fake.job(),
        'company_name': fake.company(),
        'annual_income': round(random.uniform(20000, 120000), 2),
        'credit_card_number': fake.credit_card_number(card_type=None),
        'subscription_end': fake.date_between(start_date='today', end_date='+2y')
    })

# Generate Employees Data
employees = []
for _ in range(NUM_EMPLOYEES):
    employees.append({
        'employee_id': fake.uuid4(),
        'name': fake.name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'address': fake.address().replace("\n", ", "),
        'city': fake.city(),
        'hire_date': fake.date_between(start_date='-10y', end_date='today'),
        'job_title': random_job(),
        'salary': round(random.uniform(3000, 12000), 2),
        'store_location': fake.city(),
        'is_active': random.choice([True, False])
    })

# Generate Products Data
products = []
for _ in range(NUM_PRODUCTS):
    products.append({
        'product_id': fake.uuid4(),
        'product_name': fake.word().capitalize() + ' ' + random_category(),
        'category': random_category(),
        'price': round(random.uniform(1, 500), 2),
        'stock_quantity': random.randint(0, 1000)
    })

# Generate Sales Transactions Data
transactions = []
for _ in range(NUM_TRANSACTIONS):
    product = random.choice(products)
    quantity = random.randint(1, 5)
    transactions.append({
        'transaction_id': fake.uuid4(),
        'user_id': random.choice(customers)['user_id'],
        'product_id': product['product_id'],
        'purchase_date': fake.date_time_between(start_date='-2y', end_date='now'),
        'quantity': quantity,
        'total_amount': round(product['price'] * quantity, 2),
        'payment_method': random_payment()
    })

# Generate Inventory Log Data
inventory_logs = []
for product in products:
    for _ in range(10):  # 10 changes per product
        inventory_logs.append({
            'product_id': product['product_id'],
            'timestamp': fake.date_time_between(start_date='-2y', end_date='now'),
            'stock_change': random.randint(-20, 100),
            'description': random_description()
        })

# Generate Payment History Data
payment_histories = []
for customer in customers:
    for _ in range(random.randint(1, 3)):
        payment_histories.append({
            'payment_id': fake.uuid4(),
            'user_id': customer['user_id'],
            'payment_date': fake.date_time_between(start_date='-2y', end_date='now'),
            'amount': round(random.uniform(5, 5000), 2),
            'payment_method': random_payment()
        })

# Save to CSV
pd.DataFrame(customers).to_csv('customers.csv', index=False)
pd.DataFrame(employees).to_csv('employees.csv', index=False)
pd.DataFrame(products).to_csv('products.csv', index=False)
pd.DataFrame(transactions).to_csv('transactions.csv', index=False)
pd.DataFrame(inventory_logs).to_csv('inventory_logs.csv', index=False)
pd.DataFrame(payment_histories).to_csv('payment_histories.csv', index=False)

print("Dataset generated: customers.csv, employees.csv, products.csv, transactions.csv, inventory_logs.csv, payment_histories.csv")