import configparser
import boto3
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Fetches data from logs to staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# Inserts data from staging to fact and dimenstions tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# Reads Redshift cluster and DB configuration from file, establishes connection cursor
# Runs ETL
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn) # Runs ETL from JSON to staging tables
    insert_tables(cur, conn) # Runs ETL from staging to fact and dims tables

    conn.close()


if __name__ == "__main__":
    main()
