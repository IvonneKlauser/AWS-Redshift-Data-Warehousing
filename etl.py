import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Load data in JSON files from S3 to staging tables on Redshift specified in copy_table_queries 
        
        Arguments:
            cur - PostgreSQL cursor object
            conn - psycopg2 connection instance
        
        Returns:
            None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Load data from staging tables to analytics tables specified in insert_table_queries
        
        Arguments:
            cur - PostgreSQL cursor object
            conn - psycopg2 connection instance
        
        Returns:
            None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Read config data
        Load data in JSON files from S3 to staging tables on Redshift specified in copy_table_queries
        Load data from staging tables to analytics tables specified in insert_table_queries
        
        Arguments:
            None
        
        Returns:
            None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()