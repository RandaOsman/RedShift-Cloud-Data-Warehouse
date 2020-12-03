import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
call the drop queries via drop list that defined in sql_queries.py to drop the DWH tables if exist incase if we need to alter the tables for any changes
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
"""
call the create query statments via create tables list that defined in sql_queries.py to create the DWH tables  the tables
"""

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
connect to the DB that hosted on amazon by call the its parater from the config file dwh.conf

[CLUSTER]
HOST= 'redshift-cluster-1.cqdrnmc2fjuv.us-west-2.redshift.amazonaws.com'
DB_NAME= 'dwh'
DB_USER='awsuser'
DB_PASSWORD='Passw0rd'
DB_PORT= '5439'

"""

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