# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from py4j.java_gateway import java_import

# spark = SparkSession.builder \
#     .appName("SentimentDetector")\
#      .master("local[*]")\
#     .config("spark.driver.memory","8G")\
#      .config("spark.driver.maxResultSize", "2G")\
#     .config("spark.jars", "/Users/maziyar/anaconda3/envs/spark/lib/python3.6/site-packages/sparknlp/lib/sparknlp.jar")\
#     .config("spark.driver.extraClassPath", "/Users/maziyar/anaconda3/envs/spark/lib/python3.6/site-packages/sparknlp/lib/sparknlp.jar")\
#     .config("spark.executor.extraClassPath", "/Users/maziyar/anaconda3/envs/spark/lib/python3.6/site-packages/sparknlp/lib/sparknlp.jar")\
#     .config("spark.kryoserializer.buffer.max", "500m")\
#     .getOrCreate()

# spark = SparkSession.builder \
#     .appName("ner")\
#     .master("local[4]")\
#     .config("spark.driver.memory","8G")\
#     .config("spark.driver.maxResultSize", "2G") \
#     .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.4")\
#     .config("spark.kryoserializer.buffer.max", "500m")\
#     .getOrCreate()


# Create SparkSession
# Create a SparkConf object
# conf = SparkConf().setAppName("MyApp") \
#             .setMaster("local[2]") \
#             .setSparkHome("C:\\apps\\opts\\spark-3.3.2-bin-hadoop3") \
# Create a SparkSession object
# spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
# .config("spark.jars", "C:\\apps\\opts\\spark-3.3.2-bin-hadoop3\\jars") \

java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
spark._jvm.com.package.Class

blog_rdd = spark.sparkContext.textFile("data/blogtexts")


# print(blog_rdd.take(5))
def takes(rdd, noOfLines):
    i = 0
    # print(rdd.collect())
    for element in rdd.collect():
        print(element)
        i = i + 1
        if i == noOfLines:
            break


# Q1: Convert all words in a rdd to lowercase and split the lines of a document using space.
def lowercase(lines):
    lines = lines.lower()
    lines = lines.split()
    return lines


#rdd1 = blog_rdd.flatMap(lowercase)
# rdd1 = blog_rdd.map(lambda line: line.lower()).map(lambda line: line.split())
#takes(rdd1, 4)

