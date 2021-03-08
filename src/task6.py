from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def write_file(accommodates):
       with open("../output/out_2_4.txt", "a") as f:
                     f.write(str(accommodates))
spark:SparkSession = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()
hotelDF=spark.read.parquet("../input/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")
min_max = hotelDF.agg(F.min(hotelDF.price),F.max(hotelDF.review_scores_value)).head()
col_min_price = min_max[0]
col_max_rating = min_max[1]
data = hotelDF.filter((F.col("price") == col_min_price) & (F.col("review_scores_value") == col_max_rating )).collect()
accommodates = data['accommodates']
write_file(accommodates)
