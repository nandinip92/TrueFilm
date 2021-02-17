import findspark

findspark.init()
import logging
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Setup logging config and logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s (%(filename)s): %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Global constant logger
LOGGER = logging.getLogger(__name__)

class CreateSparkSession:
    def __init__(self):
        self.spark = self._create_spark_session_()

    def _create_spark_session_(self):
        """
        Creating Spark session
        """
        jars = [
                        "Scripts/JARS/postgresql-42.2.18.jar",
                        "Scripts/JARS/spark-xml_2.11-0.11.0.jar",
                ]
        try:
            conf = (SparkConf().setAppName("movie_ratings_load")
                        .set('spark.jars', ",".join(jars))
                        .set("spark.driver.extraClassPath", ":".join(jars))
                   )
            spark = SparkSession.builder.master("local").config(conf=conf).getOrCreate()
            LOGGER.info("Spark session created...!!!")
        except Exception as e:
            LOGGER.critical(f"Error in creating Spark Session")
            LOGGER.critical(f"ERROR:{e}")
        return spark

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        main()
    else:
        # Removes file name
        main(sys.argv[1:])
