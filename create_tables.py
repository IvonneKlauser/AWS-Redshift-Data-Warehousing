"""Runs sql statements that drop and create tables for sparkify database"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import pandas as pd
import boto3
import json


def drop_tables(cur, conn):
    """
    Drop tables with names specified in the drop_table_queries if they exist
    
    Arguments:
        cur - PostgreSQL cursor object
        conn - psycopg2 connection instance
        
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables specified in create_table_queries
    
    Arguments:
        cur - PostgreSQL cursor object
        conn - psycopg2 connection instance
        
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Drop tables with names specified in the drop_table_queries if they exist
    and subsequently create tables specified in create_table_queries and close connection
    
    Arguments:
        None
        
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()