from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, sum, col
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('02-pyspark.interview').getOrCreate()
data = [("James", "Sales", 2000),
        ("sofy", "Sales", 3000),
        ("Laren", "Sales", 4000),
        ("Kiku", "Sales", 5000),
        ("Sam", "Finance", 6000),
        ("Samuel", "Finance", 7000),
        ("Mausam", "Marketing", 12000),
        ("Lamba", "Marketing", 13000),
        ("Jogesh", "HR", 14000),
        ("Mannu", "HR", 15000)
        ]
emp_schema = StructType(
    [StructField('name', StringType()), StructField('dept_name', StringType()), StructField('salary', StringType())])
df = spark.createDataFrame(data, schema=emp_schema)
print(df.show())
print(df.printSchema())
print(df.describe().show())
df.createOrReplaceTempView('emp')
query='''select name,dept_name,salary,
min(salary) over(partition by dept_name) as min_salary,
max(salary) over(partition by dept_name) as max_salary,
sum(salary) over(partition by dept_name) as cumulative_sum
from emp'''
result=spark.sql(query)
print(result.show())
