import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import re
import configparser
import logging

# Setup logging config and logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s (%(filename)s): %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Global constant logger
LOGGER = logging.getLogger(__name__)

def create_database(conn_details, new_database):
    """
    - Creates and connects to the true_film_db
    - Returns the connection and cursor to true_film_db
    """
    # connect to default database
    try:
        conn = psycopg2.connect(user=conn_details['username'],
                                password=conn_details['pwd'],
                                host=conn_details['host'],
                                port=conn_details['port']
                               )
        conn.set_session(autocommit=True)
        conn.set_client_encoding('UTF8')
        LOGGER.info("Connected to Postgres....")
    except psycopg2.Error as pe:
        LOGGER.critical("Could not make connection to the Postgres database")
        LOGGER.critical(f"Error: {pe}")
    try:
        cur = conn.cursor()
    except psycopg2.Error as ce:
        LOGGER.critical("Error: Could not get cursor to the Database")
        LOGGER.critical(f"Error: {ce}")

    # create true_film database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS "+new_database)
        LOGGER.info(f"{new_database} already exists... Droped {new_database}...")
    except psycopg2.Error as pe:
        LOGGER.warning(pe)
    try:
        cur.execute("CREATE DATABASE "+new_database+" WITH ENCODING 'utf8' TEMPLATE template0",new_database)
        LOGGER.info(f"Created {new_database} Successfully...")
    except psycopg2.Error as e:
        LOGGER.critical(e)


    # close connection to default database
    conn.close()

    # connect to true_film database
    try:
        conn = psycopg2.connect(database=new_database,
                                user=conn_details['username'],
                                password=conn_details['pwd'],
                                host=conn_details['host'],
                                port=conn_details['port']
                               )
        conn.set_client_encoding('UTF8')
    except psycopg2.Error as pe:
        LOGGER.critical(f"Error: Could not make connection to '{new_database}' Postgres database")
        LOGGER.critical(pe)
    try:
        cur = conn.cursor()
    except psycopg2.Error as ce:
        LOGGER.critical(f"Error: Could not make connection to '{new_database}' Postgres database")
        LOGGER.critical(ce)

    return cur, conn


def drop_tables(cur, conn, new_database):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            LOGGER.critical(e)
        conn.commit()


def create_tables(cur, conn, new_database):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        #LOGGER.info(query)
        try:
            cur.execute(query)
            table_name = re.findall(r'EXISTS\ (.+?)\s*\(',query)
            #LOGGER.info(f"----> Table_Name :'{table_name[0]}")
            LOGGER.info(f"'{table_name[0]}' table created successfully...")
        except psycopg2.Error as e:
            LOGGER.critical(e)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the true_film database.

    - Establishes connection with the true_film database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read("Scripts/PostgresCredentials.cfg")

    connection_details = {'username' : config["pg_connections"]["username"],
                          'pwd' : config["pg_connections"]["password"],
                          'host' : config["pg_connections"]["host"],
                          'port' : config["pg_connections"]["port"]
                         }
    new_database = config["Create_database"]["database_name"]

    cur, conn = create_database(connection_details, new_database)
    drop_tables(cur, conn, new_database)
    create_tables(cur, conn, new_database)
    conn.close()

if __name__ == "__main__":
    main()
