# Importing necessary modules
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Function to drop existing tables to clean the database
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()  # Committing the transaction

# Function to create new tables in the database
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()  # Committing the transaction

# Main function to control the flow of table creation and deletion
def main():
    # Initializing a configuration parser
    config = configparser.ConfigParser()
    
    # Reading database configuration from file
    config.read('dwh.cfg')
    
    # Establishing a connection to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    # Creating a cursor object to interact with the database
    cur = conn.cursor()
    
    # Dropping existing tables
    drop_tables(cur, conn)
    
    # Creating new tables
    create_tables(cur, conn)
    
    # Closing the database connection
    conn.close()

# Ensuring the main function is called when the script is executed
if __name__ == "__main__":
    main()
