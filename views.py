import pymysql
import os

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


def create_views():
    try:
        cursor.execute("""
        CREATE VIEW total_daily_transactions AS
        SELECT
            DATE(timestamp) AS date,
            SUM(CASE WHEN medium = 'card' THEN ABS(amount) ELSE 0 END) AS card_absolute,
            -(SUM(CASE WHEN medium = 'card' AND type = 'False' THEN -amount
                     WHEN medium = 'card' AND type = 'True' THEN amount ELSE 0 END)) AS card_net,
            SUM(CASE WHEN medium = 'online' THEN ABS(amount) ELSE 0 END) AS online_absolute,
            -(SUM(CASE WHEN medium = 'online' AND type = 'False' THEN -amount
                     WHEN medium = 'online' AND type = 'True' THEN amount ELSE 0 END)) AS online_net,
            SUM(CASE WHEN medium = 'transfer' THEN ABS(amount) ELSE 0 END) AS transfer_absolute,
            -(SUM(CASE WHEN medium = 'transfer' AND type = 'False' THEN -amount
                     WHEN medium = 'transfer' AND type = 'True' THEN amount ELSE 0 END)) AS transfer_net
        FROM transactions
        GROUP BY DATE(timestamp)
        ORDER BY DATE(timestamp) ASC;
        """)

        cursor.execute("""
        CREATE OR REPLACE VIEW monthly_transaction_summary AS
        SELECT
            DATE_FORMAT(t.timestamp, '%Y-%m-01') AS month,
            c.client_email,
            COUNT(t.transaction_id) AS transaction_count,
            SUM(ABS(t.amount)) AS total_amount
        FROM transactions t
        JOIN accounts a ON t.account_id = a.account_id
        JOIN clients c ON a.client_id = c.client_id
        GROUP BY DATE_FORMAT(t.timestamp, '%Y-%m-01'), c.client_email
        ORDER BY month ASC, c.client_email ASC;
        """)

        cursor.execute("""
        CREATE OR REPLACE VIEW monthly_high_debits AS
        SELECT
            DATE_FORMAT(t.timestamp, '%Y-%m-01') AS month,
            t.account_id,
            SUM(t.amount) AS total_debits,
            COUNT(t.transaction_id) AS transaction_count
        FROM transactions t
        WHERE t.type = 'true'
        GROUP BY DATE_FORMAT(t.timestamp, '%Y-%m-01'), t.account_id
        HAVING total_debits > 10000
        ORDER BY month ASC, t.account_id ASC;
        """)

        database.commit()
    except pymysql.MySQLError as e:
        database.rollback()


