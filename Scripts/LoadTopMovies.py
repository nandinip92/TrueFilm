import findspark

findspark.init()
import pyspark
import logging
import json
import sys
import os
from pyspark.sql.functions import trim,regexp_replace,col
from Scripts.CreateSparkSession import CreateSparkSession
from Scripts.ConnectToDatabase import ConnectToDatabase
from Scripts.sql_queries import select_query_postgres,top_movies_copy_query

# Setup logging config and logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s (%(filename)s): %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Global constant logger
LOGGER = logging.getLogger(__name__)

class LoadTopMovies:
    def __init__(self, wiki_xml, db_name):
        self.database = db_name
        self.conn_file = "./Scripts/truefilmdb_conn_details.json"
        self.wiki_xml = wiki_xml
        self.session = CreateSparkSession()
        self._get_connection_data_()
        self.process_wiki_xml()
        self._calculate_top_ratio_movie_()
        self.db = ConnectToDatabase(self.database)
        self._load_top_movies_()
        self.db.conn.close()
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

    def process_wiki_xml(self):
        """
        Processing enwiki-latest-abstract.xml file
        """
        LOGGER.info(f"Inferring {self.wiki_xml} file schema")
        abstract_xml= self.session.spark.read.format("com.databricks.spark.xml")\
                    .options(inferSchema="True", rootTag="feed", rowTag="doc")\
                    .load(self.wiki_xml)
        abstract_xml.printSchema()
        xml_df = abstract_xml.select ("url","title","abstract")
        df = xml_df.withColumn("title", regexp_replace(col("title"), "^Wikipedia:", ""))

        df = df.withColumn('url', trim(df.url))\
                    .withColumn('title', trim(df.title))\
                    .withColumn('abstract', trim(df.abstract))

        self.wiki_df = df.select("url","title","abstract")\
                            .withColumnRenamed("url", "wiki_url")\
                            .withColumnRenamed("abstract","wiki_abstract")

    def _calculate_top_ratio_movie_(self):
        """
        Calculating budget to Revenue Ratio and
            collecting top 1000 movies with higest ratio
        """
        # Reading data from postgres
        url = f'jdbc:postgresql://{self.host}:{self.port}/{self.database}'
        properties = {"user": self.username,"password": self.pwd,"driver": "org.postgresql.Driver"}

        query = f"({select_query_postgres}) as movies_table"
        self.movies_data = self.session.spark.read.jdbc(url=url,table=query,properties=properties)
        join_df = self.movies_data.join(self.wiki_df,on=['title'], how='inner')
        join_df.createOrReplaceTempView("movies_data")
        #Calculating Ratio
        movies_ratio_sql = "SELECT title,budget,EXTRACT(YEAR FROM release_date) as year ,revenue, rating, \
                CASE WHEN revenue>0 THEN budget/revenue \
                     ELSE -999.999 \
                END as ratio, \
                production_companies,wiki_url,wiki_abstract\
                FROM movies_data"
        movies_ratio = self.session.spark.sql(movies_ratio_sql)
        movies_ratio.createOrReplaceTempView("movies_ratio")
        LOGGER.info("Budget to Revenue ratio calculate...!!!")
        top_movies_sql= "SELECT dr.title,dr.budget, dr.year ,dr.revenue,dr.rating,dr.ratio,\
                            dr.production_companies,dr.wiki_url,wiki_abstract\
                            FROM ( SELECT *, DENSE_RANK() OVER(ORDER BY ratio DESC) as ratio_ranking\
                                    FROM movies_ratio ORDER BY ratio_ranking ASC) as dr \
                                WHERE dr.ratio_ranking<=1000"
        self.top_movies = self.session.spark.sql(top_movies_sql)
        LOGGER.info("Collected top 1000 movies with highest ratio...!!!")

    def _load_top_movies_(self):
        """
        Function to load Top 1000 movie details into top_movies table
        """
        final_df = self.top_movies.toPandas()
        final_df['budget'] = final_df['budget'].astype(float)
        final_df['revenue'] = final_df['revenue'].astype(float)
        final_df['ratio']= final_df['ratio'].astype(float)
        final_df['year']=final_df['year'].fillna(-9999)
        final_df['year']=final_df['year'].astype(int)
        ###
        ### Dropping duplicate rows based on all columns.
        ###
        final_df = final_df.drop_duplicates()
        output_dir = './Scripts/ProcessedData'
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        file_name = 'top_movies.csv'


        top_movies_file = os.path.join(output_dir, file_name)

        final_df.to_csv(top_movies_file, sep=",", index=False, encoding="utf-8")

        ####
        #### For Bulk Loading
        ####

        m_file = open(top_movies_file, "rb")
        #print(type())
        try:
            self.db.cur.copy_expert(sql=top_movies_copy_query, file=m_file)
            m_file.close()
            LOGGER.info("Finished loading true_film_db.top_movies table")
        except psycopg2.Error as e:
            LOGGER.critical(f"Error in Inserting file :{top_movies_file} into MOVIES_METADATA Table")
            LOGGER.critical(e)

def main():
    sd = LoadTopMovies("./Datasets/enwiki-latest-abstract/enwiki-latest-abstract.xml", "true_film_db")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        main()
    else:
        # Removes file name
        main(sys.argv[1:])
