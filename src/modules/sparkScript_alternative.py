from pyspark.context       import SparkContext
from pyspark.sql.session   import SparkSession
from pyspark.sql.functions import split, explode

sc                = SparkContext('local')
spark             = SparkSession(sc)

movies            = spark.read.csv("../data/MovieLensDataset/movies.csv",  header=True)
ratings           = spark.read.csv("../data/MovieLensDataset/ratings.csv", header=True)

### To calculate popularity score as average rating received from users given a movie
moviePopularity   = ratings.groupby('movieId').agg({"rating": "sum", "userId": "count"})
moviePopularity   = moviePopularity.withColumn('popularity', moviePopularity['sum(rating)'] / moviePopularity['count(userId)'])

### Join movie's popularity with movies title and genre information
movies_popularity_WithName = movies.join( moviePopularity, movies.movieId == moviePopularity.movieId ).orderBy('popularity', ascending=True)
movies_popularity_WithName.toPandas().to_csv('../results/movies_popularity_alternative.csv', index=False)

### Collapse genres information into multiple rows and sum
genrePopularity   = movies.withColumn('genres', explode(split('genres','\|'))) \
                          .join( moviePopularity, movies.movieId == moviePopularity.movieId ) \
                          .groupby('genres').agg({"sum(rating)": "sum", "count(userId)": "sum"})
genrePopularity   = genrePopularity.withColumn('popularity', genrePopularity['sum(sum(rating))'] / genrePopularity['sum(count(userId))']) \
                                   .orderBy('popularity', ascending=False)
genrePopularity.toPandas().to_csv('../results/genre_popularity_alternative.csv', index=False)

print('done.')