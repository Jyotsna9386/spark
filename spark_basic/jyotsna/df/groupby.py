from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min, mean, count

spark = SparkSession.builder.appName('basic').getOrCreate()
df = spark.read.csv('employees.csv', inferSchema=True, header=True)
print(df.show())

df1=df.groupBy("dept").agg(sum("salary").alias("sum_salary"))
print(df1.show(truncate=False))
