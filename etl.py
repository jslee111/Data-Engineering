import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Function to load data into staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)  # Execute each query in the list
        conn.commit()  # Commit the transaction

# Function to insert data from staging tables to analytical tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)  # Execute each query in the list
        conn.commit()  # Commit the transaction

# Main function to control the flow of data loading
def main():
    config = configparser.ConfigParser()  # Initialize configparser
    config.read('dwh.cfg')  # Read configuration file

    # Establish connection to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()  # Create a cursor object

    load_staging_tables(cur, conn)  # Load data into staging tables
    insert_tables(cur, conn)  # Insert data into analytical tables

    conn.close()  # Close the database connection

# Entry point of the script
if __name__ == "__main__":
    main()  # Call the main function
