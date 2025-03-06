import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_queries_for_check


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Running the following query:")
        print(query)
        cur.execute(query)
        conn.commit()

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