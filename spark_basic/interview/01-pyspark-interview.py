from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('01-pyspak-interview').getOrCreate()
data = [
    (1, "Gaurav", ["Pune", "Bangalore", "Hyderabad"]),
    (2, "Rishabh", ["Mumbai", "Bangalore", "Pune"])
]
schema = ['id', 'name', 'locations']
df = spark.createDataFrame(data, schema=schema)
print(df.show())
print(df.printSchema())
print(df.describe().show())
df_explode = df.select(df['id'], df['name'], F.explode(df['locations']))
print(df_explode.show())