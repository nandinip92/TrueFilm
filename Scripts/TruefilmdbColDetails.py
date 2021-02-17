import datetime
import json
import numpy as np
import findspark
findspark.init()
import pyspark
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
    ArrayType
)

## Stage table Columns
### movie_links table
links_table_cols = {'movieId' : 'movie_id', 'imdbId' :'imdb_id', 'tmdbId':'tmdb_id'}
links_datatypes = {'movie_id': np.int64, 'imdb_id': np.int64 ,'tmdb_id': np.int64}


## For movies_metadata table
movies_req_cols = ['id' ,'imdb_id' ,'genres','original_title',
                'title',
                'budget',
				'release_date' ,
                'revenue',
                'production_companies']

movies_table_cols = { 'id' : 'tmdb_id'}

movies_datatypes = {'tmdb_id':np.int64, 'imdb_id':np.int64,'budget' : np.float64, 'revenue' : np.float64, 'release_date': datetime.datetime}

json_columns = ['genres','production_companies']


#Ratings TABLE

movie_rating_schema = StructType([
  StructField("user_id", IntegerType(), False),
  StructField("movie_id", IntegerType(), False),
  StructField("rating", DoubleType(), False),
  StructField("timestamp", TimestampType(), False)]
)
