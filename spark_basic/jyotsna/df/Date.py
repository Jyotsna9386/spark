from pyspark.sql import SparkSession
from pyspark.sql.functions import add_months,current_date,current_timestamp,date_add,date_format,date_sub,datediff,year,month,dayofmonth


spark = SparkSession.builder.appName('Date').getOrCreate()
emp = [(1, "AAA", "dept1", 1000, "2023-09-01 15:12:13"),
       (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
       (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
       (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
       (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
       (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
       (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
       (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
       (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
       (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
d_schema=["id", "name", "dept", "salary", "date"]
df = spark.createDataFrame(emp,schema=d_schema)
print(df.show())
# df1=df.select('date').withColumn('next_month',add_months('date',1))
# print(df1.show())
# df2=df.withColumn('current_date',current_date()).select(df['id'],'current_date')
# print(df2.show())
# df3=df.withColumn('current_timestamp',current_timestamp()).select(df['id'],'current_timestamp')
# print(df3.show(truncate=False))
# df4=df.select(df['date']).withColumn('FormatDate',date_format('date','dd/MM/yyyy'))
# print(df4.show())
# df5=df.select(df['date']).withColumn('current_date',current_date()).withColumn('Date_diff',datediff('current_date','date'))
# print(df5.show(truncate=False))
# df6=df.select(df['date']).withColumn('Year',year('date'))
# print(df6.show())
# df6=df.select(df['date']).withColumn('month',month('date'))
# print(df6.show())
df6=df.select(df['date']).withColumn('days',dayofmonth('date'))
print(df6.show())
