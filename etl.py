import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_queries_for_check

"""
Create staging tables that contain the raw data into Redshift
Parameters:
    - cur = database cursor that executes the database queries
    - conn = A database connection object from psycopg2 that establishes the connection to the
    database. The connection requires credentials from AWS Redshift
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
Insert the data into fact and dimension tables
Parameters:
    - cur = database cursor that executes the database queries
    - conn = A database connection object from psycopg2 that establishes the connection to the
    database. The connection requires credentials from AWS Redshift
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Running the following query:")
        print(query)
        cur.execute(query)
        conn.commit()
"""
Select all the tables available for verifications
Parameters:
    - cur = database cursor that executes the database queries
    - conn = A database connection object from psycopg2 that establishes the connection to the
    database. The connection requires credentials from AWS Redshift
"""
def select_tables(cur, conn):
    for query in select_queries_for_check:
        print(query)
        cur.execute(query)
        result=cur.fetchone()
        print(result[0])
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("load_staging_tables...")
    load_staging_tables(cur, conn)
    print("insert_tables...")
    insert_tables(cur, conn)
    print("select_queries_for_check...")
    select_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()