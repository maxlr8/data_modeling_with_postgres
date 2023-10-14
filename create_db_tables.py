import psycopg2
import os

from sql_queries import *
from dotenv import load_dotenv

# Setting default project directory.
ENV_PATH = ".env"
load_dotenv(ENV_PATH)


def create_database():
    """
    This function creates a database named 'sparkify'.
    And returns the cursor and connection references.
    """

    # Create a connection to the default postgres database and create a cursor.
    con = psycopg2.connect(
        host="127.0.0.1",
        dbname="postgres",
        user="postgres",
        password=os.environ["PSQL_PASS"],
    )

    con.set_session(autocommit=True)
    cur = con.cursor()

    # Create a database called 'sparkify' with 'utf-8' encoding.
    cur.execute("DROP DATABASE IF EXISTS sparkify;")
    cur.execute("CREATE DATABASE sparkify WITH ENCODING 'utf-8' TEMPLATE template0")

    # Close the coonection to the default database.
    con = psycopg2.connect(
        host="127.0.0.1",
        dbname="sparkify",
        user="postgres",
        password=os.environ["PSQL_PASS"],
    )
    cur = con.cursor()

    return cur, con


def create_tables(cur, con):
    """
    This function runs all the CREATE TABLE queries defined in the queries.py.
    """

    for query in create_table_queries:
        cur.execute(query)
        con.commit()


def drop_tables(cur, con):
    """
    This function runs all the DROP TABLE queries defined in the queries.py.
    """

    for query in drop_table_queries:
        cur.execute(query)
        con.commit()


def main():
    """
    The main function.
    """

    cur, con = create_database()

    drop_tables(cur, con)
    print("\nDropped all the previous tables with conflicting names. - songplays\nusers\nsongs\nartists\ntime\n\n")

    create_tables(cur, con)
    print("Created all the requisite tables for data modelling. - songplays\nusers\nsongs\nartists\ntime\n\n")

    cur.close()
    con.close()


if __name__ == "__main__":
    main()
