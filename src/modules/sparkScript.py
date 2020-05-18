from pyspark.context       import SparkContext
from pyspark.sql.session   import SparkSession
from pyspark.sql.functions import split, explode

sc                = SparkContext('local')
spark             = SparkSession(sc)

movies            = spark.read.csv("../data/MovieLensDataset/movies.csv",  header=True)
ratings           = spark.read.csv("../data/MovieLensDataset/ratings.csv", header=True)

### To make sure number of rating is counted once for each user, drop duplicates by ['userId', 'movieId']
### Treat total number of ratings received for a movie as its popularity score
distinctUserMovie = ratings.dropDuplicates(['userId', 'movieId']).groupby('movieId').count().withColumnRenamed('count', 'popularity')

### Join movie's popularity with movies title and genre information
movies_popularity = movies.join( distinctUserMovie, movies.movieId == distinctUserMovie.movieId ).orderBy('popularity', ascending=True)
movies_popularity.toPandas().to_csv('../results/movies_popularity.csv', index=False)

### Collapse genres information into multiple rows and sum
genrePopularity   = movies_popularity.withColumn('genres', explode(split('genres','\|'))) \
                                     .groupby('genres').agg({"popularity": "sum"}).orderBy('sum(popularity)', ascending=False) \
                                     .withColumnRenamed('sum(popularity)', 'popularity')
genrePopularity.toPandas().to_csv('../results/genre_popularity.csv', index=False)

print('done.')