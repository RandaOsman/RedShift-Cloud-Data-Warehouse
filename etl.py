import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
call the copy function from sql_queries.py via copy_table_queries list to populate the staging tables
"""

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
"""
call the insert function from sql_queries.py via insert_table_queries list to populate the DWH tables
"""


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
connect to the DB that hosted on amazon by call the its parater from the config file dwh.conf

"""

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
