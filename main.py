import os
import pymysql
import pandas as pd
import requests
import time
from io import StringIO
from views import create_views

ADMIN_API_KEY = os.getenv('ADMIN_API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = int(os.getenv('DB_PORT'))
DB_USERNAME = os.getenv('DB_USERNAME')

database = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USERNAME,
    passwd=DB_PASSWORD,
    db=DB_NAME
)
cursor = database.cursor()


def setup_tables():
    table_queries = [
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_id INT PRIMARY KEY,
            client_id VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS clients (
            client_id VARCHAR(255) PRIMARY KEY,
            client_name VARCHAR(255),
            client_email VARCHAR(255),
            client_birth_date DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INT PRIMARY KEY,
            timestamp DATETIME,
            account_id INT,
            amount DECIMAL(10, 2),
            type VARCHAR(255),
            medium VARCHAR(255)
        );
        """
    ]
    for query in table_queries:
        try:
            cursor.execute(query)
        except pymysql.MySQLError as e:
            print(f"Error setting up tables: {e}")
            database.rollback()


def insert_accounts(accounts_df):
    query = "INSERT INTO accounts(account_id, client_id) VALUES (%s, %s);"
    for _, row in accounts_df.iterrows():
        cursor.execute(query, (int(row['account_id']), str(row['client_id'])))


def insert_clients(clients_df):
    query = "INSERT INTO clients(client_id, client_name, client_email, client_birth_date) VALUES (%s, %s, %s, %s);"
    for _, row in clients_df.iterrows():
        cursor.execute(query, (
            str(row['client_id']),
            str(row['client_name']),
            str(row['client_email']),
            pd.to_datetime(row['client_birth_date']).date()
        ))


def insert_transactions(transactions_df):
    query = """
    INSERT INTO transactions(transaction_id, timestamp, account_id, amount, type, medium) 
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    for _, row in transactions_df.iterrows():
        cursor.execute(query, (
            int(row['transaction_id']),
            pd.to_datetime(row['timestamp']),
            int(row['account_id']),
            float(row['amount']),
            str(row['type']),
            str(row['medium'])
        ))


def data_import():
    headers = {'Authorization': f'Bearer {ADMIN_API_KEY}'}
    accounts_url = f"{API_BASE_URL}/download/accounts.csv"
    clients_url = f"{API_BASE_URL}/download/clients.csv"
    transactions_url = f"{API_BASE_URL}/transactions"

    try:
        accounts_df = pd.read_csv(StringIO(requests.get(accounts_url, headers=headers).content.decode('utf-8')))
        clients_df = pd.read_csv(StringIO(requests.get(clients_url, headers=headers).content.decode('utf-8')))
    except Exception as e:
        print(f"Error downloading or reading CSV data: {e}")
        return 0, 0, 0

    transactions_df = pd.DataFrame()
    page = 0
    while True:
        paginated_url = f"{transactions_url}?page={page}"
        try:
            response = requests.get(paginated_url, headers=headers)
            response.raise_for_status()
            if not response.json():
                break
            page_data = pd.json_normalize(response.json())
            transactions_df = pd.concat([transactions_df, page_data], ignore_index=True)
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            time.sleep(5)
            continue

    transactions_df['timestamp'] = pd.to_datetime(transactions_df['timestamp'], errors='coerce')
    transactions_df['amount'] = pd.to_numeric(transactions_df['amount'], errors='coerce')
    transactions_df = transactions_df.dropna(subset=['timestamp', 'amount'])
    transactions_df = transactions_df[transactions_df['transaction_id'].apply(lambda x: str(x).isdigit())]
    transactions_df['transaction_id'] = transactions_df['transaction_id'].astype(int)
    transactions_df = transactions_df[transactions_df['transaction_id'] <= 30000]
    transactions_df = transactions_df.drop_duplicates(subset='transaction_id')

    try:
        insert_accounts(accounts_df)
        insert_clients(clients_df)
        insert_transactions(transactions_df)
        database.commit()
        return len(clients_df), len(accounts_df), len(transactions_df)
    except pymysql.MySQLError as e:
        print(f"Error inserting data: {e}")
        database.rollback()
        return 0, 0, 0


def main():
    setup_tables()
    num_clients, num_accounts, num_transactions = data_import()

    if num_clients > 0 and num_accounts > 0 and num_transactions > 0:
        try:
            create_views()
        except pymysql.MySQLError as e:
            print(f"Error creating views: {e}")

    print(f"ZYLYTY Data Import Completed [{num_clients}, {num_accounts}, {num_transactions}]")

if __name__ == "__main__":
    try:
        main()
    finally:
        cursor.close()
        database.close()
