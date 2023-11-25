from session import spark
import pyspark.sql.functions as F
data = [
 (1, "Gaurav",["Pune", "Bangalore", "Hyderabad"]),
 (2, "Rishabh",["Mumbai", "Bangalore", "Pune"])
]
# Parallelize the data
parallelized_data = spark.sparkContext.parallelize(data)
# Create a DataFrame from the parallelized data
df = parallelized_data.toDF(["id", "name", "locations"])
# Show the DataFrame
df.show()
# df_exploded = df.select(
#  df["id"],
# df["name"],
#  F.explode(df["locations"]).alias("location")
# )
# df_exploded.show()