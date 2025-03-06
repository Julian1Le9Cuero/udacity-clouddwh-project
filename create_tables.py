# where you'll create your fact and dimension tables for the star schema in Redshift.
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
Drop or delete the tables from the database in case they already exists.
Parameters:
    - cur = database cursor that executes the database queries
    - conn = A database connection object from psycopg2 that establishes the connection to the
    database. The connection requires credentials from AWS Redshift
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
Create the tables for the database.
Parameters:
    - cur = database cursor that executes the database queries
    - conn = A database connection object from psycopg2 that establishes the connection to the
    database. The connection requires credentials from AWS Redshift
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()