from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize SparkSession
#    .appName("YourEDASparkApp") - Give your application a meaningful name
#    .master("local[*]") - Use all available cores on your local machine.
#                          For a cluster, this would be "spark://<master_url>:<port>" or "yarn"
spark = SparkSession.builder \
    .appName("YourEDASparkApp") \
    .master("local[*]") \
    .config("spark.hadoop.user.name", "hadoop") \
    .getOrCreate()

print("SparkSession created successfully!\n")
print(f"Spark Version: {spark.version}")

df = spark.read.csv("data/samplesubmission.csv", header=True, inferSchema=True)
print("--- Initial DataFrame ---")
df.show()
df.printSchema()

# Assign integer type to the 'final_status' column.
print("\n--- DataFrame after casting 'final_status' to Integer ---")
df_casted = df.withColumn("final_status", col("final_status").cast("integer"))
print("\n--- Cast of 'final_status' to Integer OK ✅ ---")


# Show the result to verify the change
df_casted.show()
df_casted.printSchema()