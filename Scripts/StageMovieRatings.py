import findspark

findspark.init()
import pyspark
import logging
import json
import sys
from pyspark.sql.functions import mean,round
from Scripts.CreateSparkSession import CreateSparkSession
from Scripts.TruefilmdbColDetails import movie_rating_schema

# Setup logging config and logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s (%(filename)s): %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Global constant logger
LOGGER = logging.getLogger(__name__)

class StageMovieRatings():
    def __init__(self, ratings_files, db_name):
        self.database = db_name
        self.conn_file = "./Scripts/truefilmdb_conn_details.json"
        self.ratings_files = ratings_files
        self.session = CreateSparkSession()
        self._get_connection_data_()
        self._process_ratings_data_()
        self.session.spark.stop()

    def _get_connection_data_(self):
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

    def _process_ratings_data_(self):
        # Reading ratings.csv file in Spark Dataframe
        #   Unlike movies metadata and links files, we can have any number of users
        #   rating any number of movies (Many -to- Many). Thus using Spark for
        #   processing this file
        rdf = self.session.spark.read.schema(movie_rating_schema).options(header= 'True').csv(self.ratings_files)
        LOGGER.debug(f"File schema : {rdf.printSchema()}")

        # Selecting only required columns
        ratings = rdf.select("movie_id","rating")

        # Calculating mean ratings, collating all user inputs
        agg_ratings = ratings.groupBy("movie_id").agg(round(mean("rating"),1).alias("rating"))

        # Loading dataframe into postgres
        url = f'jdbc:postgresql://{self.host}:{self.port}/{self.database}'
        properties = {"user": self.username,"password": self.pwd,"driver": "org.postgresql.Driver"}
        agg_ratings.write.jdbc(url=url, table="movie_ratings", mode="overwrite", properties=properties)
        LOGGER.info("Finished loading true_film_db.movie_ratings table")

def main():
    sd = StageMovieRatings("./Datasets/archive/ratings*.csv", "true_film_db")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        main()
    else:
        # Removes file name
        main(sys.argv[1:])
