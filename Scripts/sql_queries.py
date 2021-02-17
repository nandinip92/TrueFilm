# DROP TABLES

movies_metadata_drop = "DROP TABLE movies_metadata CASCADE"
movie_links_drop = "DROP TABLE movie_links CASCADE"
movie_ratings_drop = "DROP TABLE movie_ratings CASCADE"
top_movies_drop = "DROP TABLE top_movies CASCADE"

# CREATE TABLES

movie_links_create = """CREATE TABLE IF NOT EXISTS movie_links (
                movie_id INT UNIQUE,
				imdb_id INT,
                tmdb_id INT,
                PRIMARY KEY (movie_id,imdb_id),
                UNIQUE(movie_id,imdb_id,tmdb_id))
"""

movies_metadata_create = """CREATE TABLE IF NOT EXISTS movies_metadata (
                tmdb_id INT,
				imdb_id INT,
                genres JSONB default '{}'::jsonb,
                original_title 	VARCHAR NOT NULL,
                title VARCHAR,
                budget NUMERIC NOT NULL,
				release_date DATE DEFAULT 'infinity'::date ,
                revenue NUMERIC NOT NULL,
                production_companies JSONB default '{}'::jsonb,
                PRIMARY KEY (tmdb_id),
                UNIQUE(tmdb_id,imdb_id,release_date))
"""

movie_ratings_create = """CREATE TABLE IF NOT EXISTS movie_ratings (
                movie_id INT PRIMARY KEY,
				rating DECIMAL(1,1) CHECK(rating<=5.0))

"""

top_movies_create = """ CREATE TABLE IF NOT EXISTS top_movies (
                    title VARCHAR,
                    budget NUMERIC NOT NULL,
                    year INT ,
                    revenue NUMERIC NOT NULL,
                    rating NUMERIC,
                    ratio NUMERIC,
                    production_companies JSONB default '{}'::jsonb,
                    wiki_url TEXT,
                    wiki_abstract TEXT,
                    PRIMARY KEY(title,year,production_companies),
                    UNIQUE(title,year,wiki_url)
                )

            """


# Using the COPY for Bulk loading
movie_links_create_copy_query ="""COPY movie_links ( movie_id, \
                                    imdb_id, \
                                    tmdb_id \
                    ) FROM STDIN WITH CSV HEADER DELIMITER AS ',' ENCODING 'UTF8'
                """

movies_metadata_copy_query = """COPY movies_metadata (tmdb_id, \
                    imdb_id, \
                    genres,\
                    original_title,\
                    title, \
                    budget, \
                    release_date, \
                    revenue, \
                    production_companies \
                    ) FROM STDIN WITH CSV HEADER DELIMITER AS ',' ENCODING 'UTF8'
                """

movie_ratings_copy_query = """COPY movie_links ( movie_id, \
                                    rating
                    ) FROM STDIN WITH CSV HEADER DELIMITER AS ',' ENCODING 'UTF8'

                        """

top_movies_copy_query = """COPY  top_movies (
                    title,\
                    budget,\
                    year,\
                    revenue,\
                    rating,\
                    ratio,\
                    production_companies,\
                    wiki_url,\
                    wiki_abstract\
                    ) FROM STDIN WITH CSV HEADER DELIMITER AS ',' ENCODING 'UTF8'"""


# QUERY LISTS

create_table_queries = [movie_links_create,
                        movies_metadata_create,
                        movie_ratings_create,
                        top_movies_create
                       ]
drop_table_queries = [ movie_links_drop,
                      movies_metadata_drop,
                      movie_ratings_drop ,
                     top_movies_drop
                     ]


###
### SELECT QUERIES for TOP 1000 movies with highest ratio
###

select_query_postgres= """SELECT lnk.movie_id,lnk.imdb_id,\
                            trim(mdta.title) as title,mdta.budget,mdta.release_date,mdta.revenue,\
                            mdta.production_companies,r.rating FROM movie_links as lnk\
                            INNER JOIN movies_metadata as mdta ON mdta.tmdb_id=lnk.tmdb_id and lnk.imdb_id = mdta.imdb_id\
                            INNER JOIN movie_ratings as r ON r.movie_id = lnk.movie_id"""
