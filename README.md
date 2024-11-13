ZyLyTy Data Engineer Challenge

This code challenge involves developing an ETL process to fetch data from a public API, clean and transform it, store it in a MySQL database, and generate specific views.

The ETL pipeline will run in Docker with Python 3.9, using environment variables provided by the evaluator.

Requirements

Connect to the API at https://public.zylyty.com/31964/ with a secret API key (provided as an environment variable) to fetch data from three endpoints:

/download/accounts.csv - CSV file
/download/clients.csv - CSV file
/transactions - JSON format
Then, connect to a MySQL database using the provided environment variables, load the data into tables, and create the following views:

total_daily_transactions - For each recorded day (in ascending order), this view shows the total (absolute and net) amount transacted for each medium type.

monthly_transaction_summary - For each customer email, this view shows the total number of transactions and the absolute sum of transaction amounts (credits and debits) per recorded month, ordered by month and customer email.

monthly_high_debits - This view shows accounts with total debits exceeding $10,000 for any given month, along with the corresponding total debits amount and debit transaction count, sorted by month and account ID.

I've structured the solution into two main scripts, main.py and views.py:

main.py: Connects to the database and sets up the required tables. It then fetches data from the API endpoints, loads the data into pandas dataframes for cleaning and validation, and inserts the cleaned data into the database. Key steps include:

Creating and defining the table schema (including data types and primary keys) for an empty database.

Fetching data, performing data cleaning (e.g., removing duplicate or invalid transactions), and counting valid records.

Inserting data into tables with transaction control (commit if successful, rollback if any errors).

views.py: Executes the SQL queries needed to create the specified views.

The process concludes by committing all transactions and closing the database connection. If you have suggestions for improving the code, feel free to reach out!
