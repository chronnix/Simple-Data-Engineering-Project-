import psycopg2
from create_tables import create_table_queries

def Create_Database():
    conn = psycopg2.connect("host=<host> dbname=<database> user=<user> password=<password>")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    cur.execute("Drop Database pandemic")
    cur.execute("Create Database Pandemic")
    
    conn.close()
    
Create_Database()


def Create_Tables():
    conn = psycopg2.connect("host=<host> dbname=<database> user=<user> password=<password>")
    cur = conn.cursor()
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

Create_Tables()



