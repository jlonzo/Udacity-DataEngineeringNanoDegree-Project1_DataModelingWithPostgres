import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    Initialize sparkifydb DB, DBConnection and DBConnection Cursor

    Returns:
    cur: DB Connection Cursor
    conn: DB Connection

    """

    # Connect to Default DB
    conn = psycopg2.connect(""" 
        host=127.0.0.1 dbname=studentdb user=student password=student""")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # Create sparkify DB and close connection
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb \
        WITH ENCODING 'utf8' TEMPLATE template0")    
    conn.close()
    
    # Connect to sparkify DB
    conn = psycopg2.connect("""
        host=127.0.0.1 dbname=sparkifydb user=student password=student""")
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur, conn):
    """ 
    Deletes each table in sparkify DB by executing the queries in
    'drop_table_queries' list.
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    Creates each table in sparkify DB by executing the queries in
    'create_table_queries' list.
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main Program:     
    1. Initialize DB and Create DBConnection
    2. Drop all tables in sparkify DB
    3. Creates all tables in sparkify DB
    4. Close DB Connection

    """
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()