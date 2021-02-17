import os
import json
import psycopg2
import logging

# Setup logging config and logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s (%(filename)s): %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Global constant logger
LOGGER = logging.getLogger(__name__)

class ConnectToDatabase:
    def __init__(self, db_name):
        self.database = db_name
        self.conn_file = "./Scripts/truefilmdb_conn_details.json"
        self._get_details_()
        self.conn,self.cur = self._establish_db_connection_()

    def _get_details_(self):
        with open(self.conn_file) as fp:
            data = json.load(fp)
        try:
            self.host = data[self.database]["host"]
            self.username = data[self.database]["username"]
            self.pwd = data[self.database]["password"]
            self.port = data[self.database]["port"]
        except KeyError as ke:
            LOGGER.critical(
                f"DB details not found for : {self.database} in connection file {self.conn_file}"
            )
            LOGGER.critical(f"Error : {ke}")

    def _establish_db_connection_(self):
        try:
            conn = psycopg2.connect(database = self.database,user=self.username,password=self.pwd,host=self.host)
            conn.set_session(autocommit=True)
            #conn.set_client_encoding('UTF8')
            LOGGER.info("Connected to Postgres....")
        except psycopg2.Error as pe:
            LOGGER.critical("Could not make connection to the Postgres database")
            LOGGER.critical(f"Error : {pe}")
        try:
            cur = conn.cursor()
            cur.execute("SET CLIENT_ENCODING TO 'UTF8';")
            LOGGER.info("cursor created....")
        except psycopg2.Error as ce:
            LOGGER.critical("Could not get curser to the Database")
            LOGGER.critical(f"Error: {ce}")
        return conn,cur

if __name__ == '__main__':
    main()
