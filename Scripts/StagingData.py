import os
import sys
import psycopg2
import datetime
import json
import logging
import pandas as pd
import numpy as np
from glob import glob
from ast import literal_eval
from Scripts.ConnectToDatabase import ConnectToDatabase
from Scripts.sql_queries import movies_metadata_copy_query
from Scripts.sql_queries import movie_links_create_copy_query
from Scripts.TruefilmdbColDetails import movies_req_cols,movies_table_cols,movies_datatypes,json_columns
from Scripts.TruefilmdbColDetails import links_table_cols,links_datatypes
# Setup logging config and logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s (%(filename)s): %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Global constant logger
LOGGER = logging.getLogger(__name__)

class StagingBase:
    """
    Class containing commonly used functions.

    This class has all functions present as staticmethods so
        an object need not be created to call a function
    """
    def __init__(self):
        pass

    @staticmethod
    def clean_dataframe(col,req_data_type,df):
        """
        Function to check validity of data.

        This function checks for invalid int & float values
            and can be further extended as needed
        """
        LOGGER.debug("------ Column : {}".format(col))
        integers = [np.int8,np.int16, np.int32, np.int64]
        floats = [np.float16, np.float32, np.float64]
        bad_indexes = list()
        if req_data_type in integers and df[col].dtypes == object:
            df[col] = df[col].fillna('-9999')
            bad_indexes = df[~df[col].str.match(r'^[0-9]+$|^\-9999$')].index.values.tolist()
        elif req_data_type in floats and df[col].dtypes == object:
            df[col] = df[col].fillna('-999.999')
            bad_indexes = df[~df[col].str.match(r'^[0-9]+\.?[0-9]*$|^\-999.999$')].index.values.tolist()
        else:
            StagingBase.handling_nulls(col,req_data_type,df)
        LOGGER.debug(f"Bad Indices : {bad_indexes}")
        return bad_indexes

    @staticmethod
    def handling_nulls(col,req_data_type,df):
        """
        Fill NULL values with default values for batch processing
        """
        integers = [np.int8,np.int16, np.int32, np.int64]
        floats = [np.float16, np.float32, np.float64]
        if req_data_type in integers:
            df[col]=df[col].fillna(-9999)
        elif req_data_type in floats:
            df[col]=df[col].fillna(-999.999)

    @staticmethod
    def process_json(col, df):
        """
        Function to convert json strings as Json Data Objects
        """
        df[col] = df[col].fillna("[]")
        df[col] = df[col].apply(lambda x: literal_eval(x))
        df[col] = df[col].apply(lambda x: json.dumps(x))

    @staticmethod
    def dtype_checker(exp_types, df):
        """
        Function for Cleaning Data
            1. Collecting columns which have different Datatypes
                Inferred datatypes in pandas may or maynot be same as
                required datatype
            2. Datetime columns need to be processed seperately
                It is an object and not one of basic datatypes like int, float
            3. Collect Bad records
        """
        bad_indices = list()
        date_cols = list()
        type_convetion_cols = dict()
        for k,v in exp_types.items():
            if (
                (k in df and df[k].dtypes != exp_types[k]) ## basic python datatypes
            ):
                ## Collect bad records
                bad_idx = StagingBase.clean_dataframe(k,v,df)
                bad_indices = list(set(bad_indices+bad_idx))
                type_convetion_cols[k] = v
            ## Seperating Datetime object from basic datatypes
            elif exp_types[k] == datetime.datetime:
                date_cols.append(k)
            else:
                StagingBase.handling_nulls(k,v,df)

        LOGGER.info(f"Date Columns {date_cols}")
        LOGGER.info(f"Type Conversion Columns {type_convetion_cols}")
        return (bad_indices,date_cols,type_convetion_cols)

    @staticmethod
    def bulk_load(copy_query, output_dir, file_name, cur, df):
        """
        Create CSV file with processed data for bulk-loading into Postgres

        In a production environment, we will be creating time-stamped folders
            and files as necessary.
        """
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        movies_file = os.path.join(output_dir, file_name)
        df.to_csv(movies_file, sep=",", index=False, encoding="utf-8")
        ## For Bulk Loading
        m_file = open(movies_file, "rb")
        try:
            cur.copy_expert(sql=copy_query, file=m_file)
            m_file.close()
        except psycopg2.Error as pe:
            LOGGER.critical(f"Error in Inserting MOVIES for file :{movies_file}")
            LOGGER.critical(f"Error : {pe}")


class StagingData:
    """
    Wrapper Class to read in relevant files, creating Staging Data

    The number of files being read can be extended based on use case
    """
    def __init__(self, movies_metadata, link_files, db_name):
        self.movies_metadata = movies_metadata
        self.link_files = glob(link_files)
        self.db_name = db_name
        ## Creating a database connection, we will be using
        ##  db.conn and db.cur for database transactions
        self.db = ConnectToDatabase(self.db_name)

        LOGGER.info(f"Link files {self.link_files}")
        self.process_links()

        LOGGER.info(f"Movies metadata File {self.movies_metadata}")
        self.process_md()
        ## Close database connection
        self.db.conn.close()

    def process_md(self):
        movies_metadata = pd.read_csv(self.movies_metadata, low_memory=False)
        ## Selecting only required columns from the given datasets
        movies_df = movies_metadata.loc[:,movies_req_cols]
        ## Renaming columns accoring to the table
        movies_df.rename(columns = movies_table_cols,inplace=True)
        LOGGER.debug(f"Movies Datatypes {movies_df.dtypes}")
        ### This is a Special case where imdb_id needs to be stripped with "tt"
        movies_df['imdb_id'] = movies_df['imdb_id'].str.lstrip("tt")
        ## Collecting bad indices
        (bad_indices,date_cols,type_convetion_cols) = StagingBase.dtype_checker(movies_datatypes,movies_df)

        ##
        ## Bad records can be extracted using index if required
        ## I am dropping bad records in this case
        ##
        if bad_indices:
            LOGGER.info(f"Bad Indices in movies_metadata {bad_indices}")
            movies_df.drop(bad_indices, axis = 0, inplace = True)

        ## Coverting Datatypes
        movies_df = movies_df.astype(type_convetion_cols)

        ## Conveting date columns
        for d_col in date_cols:
            movies_df[d_col] = pd.to_datetime(movies_df[d_col],format='%Y-%m-%d',errors = 'coerce')

        LOGGER.debug(f"Movies Datatypes {movies_df.dtypes}")

        ## Converting Json columns
        for col in json_columns:
            StagingBase.process_json(col, movies_df)

        ## Dropping duplicate rows based on all columns for bulk loading
        movies_df = movies_df.drop_duplicates()
        file_name = 'movies_data.csv'
        output_dir = './Scripts/ProcessedData'
        StagingBase.bulk_load(movies_metadata_copy_query,output_dir,file_name,self.db.cur,movies_df)
        LOGGER.info(f"Finished loading true_film_db.movies_metadata table")

    def process_links(self):
        links_df = pd.concat([pd.read_csv(f,low_memory=False) for f in self.link_files],ignore_index=True)
        ### Renaming columns accoring to the table
        links_df.rename(columns = links_table_cols,inplace=True)
        LOGGER.debug(f"Links Datatypes {links_df.dtypes}")

        ## Collecting bad indices
        (bad_indices,date_cols,type_convetion_cols) = StagingBase.dtype_checker(links_datatypes,links_df)

        ##
        ## Bad records can be extracted using index if required
        ## I am dropping bad records in this case
        ##
        if bad_indices:
            LOGGER.info(f"Bad Indices in links {bad_indices}")
            links_df.drop(bad_indices, axis = 0, inplace = True)

        ## Coverting Datatypes
        links_df = links_df.astype(type_convetion_cols)

        ## Conveting date columns
        for d_col in date_cols:
            links_df[d_col] = pd.to_datetime(links_df[d_col],format='%Y-%m-%d',errors = 'coerce')

        LOGGER.debug(f"Links Datatypes {links_df.dtypes}")

        ## Dropping duplicate rows based on all columns for bulk loading
        links_df = links_df.drop_duplicates()
        file_name = 'movie_links.csv'
        output_dir = './Scripts/ProcessedData'
        StagingBase.bulk_load(movie_links_create_copy_query,output_dir,file_name,self.db.cur,links_df)
        LOGGER.info(f"Finished loading true_film_db.movie_links table")

def main():
    sd = StagingData("./Datasets/archive/movies_metadata.csv", "./Datasets/archive/links*.csv", "true_film_db")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        main()
    else:
        # Removes file name
        main(sys.argv[1:])
