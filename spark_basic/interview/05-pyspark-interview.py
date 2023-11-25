from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import date_format, month, year, dayofmonth, days, to_date, col, Column, format_number

spark = SparkSession.builder.appName('05-pyspark-interview').getOrCreate()

data = [(
    '3000', '22-may'),
    ('5000', '23-may'),
    ('5000', '25-may'),
    ('10000', '22-june'),
    ('1250', '03-july')]
d_schema = ['revenue', 'date']
df = spark.createDataFrame(data, schema=d_schema)
print(df.show(truncate=False))
# df1 = df.select('date').withColumn('month', date_format(to_date('date', 'dd-MMM'), 'MMM'))
# df1 = df.select(df["revenue"], month(to_date(df["date"], "dd-MM-yyyy")))
# df1 = df.select(df["revenue"], date_format(to_date(df["date"], "dd-MMMM"), "MMMM"))
df=df.withColumn("Month",date_format(to_date(df["date"], "dd-MMMM"), "MMMM"))
print(df.show(truncate=False))
