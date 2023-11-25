# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
# if __name__ == '__main__':
#     print_hi('PyCharm')
# Create RDD from parallelize
# dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
# rdd=spark.sparkContext.parallelize(dataList)
# print(rdd.collect())
# 1. read rdd employee.csv and display
# 2. filter the employe having salary gt>2000
# 3. find hihest and lowest salary
# 4. find kth highest and lowest salary
# 5. find department wise highest and lowest salary
# 6. find department wise kth highest salary

# 1.  read rdd employee.csv and display
emp_rdd = spark.sparkContext.textFile("employees.csv")
# print(emp_rdd.collect())
for element in emp_rdd.collect():
    print(element)
