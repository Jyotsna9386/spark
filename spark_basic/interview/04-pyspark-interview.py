from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import count,col

spark=SparkSession.builder.appName('04').getOrCreate()
df=spark.createDataFrame([('The Shawshank Redemption', ['Drama', 'Crime']),
                            ('The Godfather', ['Drama', 'Crime']),
                            ('Pulp Fiction', ['Drama', 'Crime', 'Thriller']),
                            ('The Dark Knight', ['Drama', 'Crime', 'Thriller', 'Action']),
                            ], ["name", "genres"])
print(df.show(truncate=False))
df1=df.select(df['name'],F.explode(df['genres']).alias('genre'))
print(df1.show(truncate=False))
df2=df1.groupBy(df1['genre']).agg(F.count(df1['genre']))
print(df2.show(truncate=False))

