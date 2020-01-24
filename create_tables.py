import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    '''
    Connect to postgres and create the sparkifydb database, droping and recreating if previously created

    INPUT
    none

    OUTPUT
    cur: psycopg2 cursor object
    conn: psycopg2 sparkifydb database connection
    '''
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb;")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    '''
    Drop any existing tables in the database

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2 sparkifydb connection

    OUTPUT
    none
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create sparkifydb tables

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2

    OUTPUT
    none
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connect to posgres, create the sparkifydb databse, drop any existing tables, and create or recreate the required tables; close the database connection when finished

    INPUT
    none

    OUTPUT
    none
    '''
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
