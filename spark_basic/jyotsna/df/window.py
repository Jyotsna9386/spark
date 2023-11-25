from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, rank, dense_rank, ntile, percent_rank, lead, lag, col, avg,max,min,sum

spark = SparkSession.builder.appName('window').getOrCreate()
df = spark.read.csv('employees.csv', inferSchema=True, header=True)

windowSpecAgg = Window.partitionBy('dept').orderBy('salary')
agg_df = df.withColumn('Row_number', row_number().over(windowSpecAgg)) \
        .withColumn('Average', avg(col('salary')).over(windowSpecAgg)) \
        .withColumn('Minimum', min(col('salary')).over(windowSpecAgg)) \
        .withColumn('Maximum', max(col('salary')).over(windowSpecAgg))\
        .withColumn('Sum', sum(col('salary')).over(windowSpecAgg))
print(agg_df.show())
res_df = agg_df.where(col('Row_number') <= 2).select('dept', 'name','salary', 'Average', 'Minimum', 'Maximum')
print(res_df.show(truncate=False))
