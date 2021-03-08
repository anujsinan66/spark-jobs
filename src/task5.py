from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def write_file(avg_bathrooms,avg_bedrooms):
       with open("../output/out_2_3.txt", "a") as f:
                     f.write(str(avg_bathrooms)+','+str(avg_bedrooms))
spark:SparkSession = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()
hotelDF=spark.read.parquet("../input/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")
avg_bed_bath = hotelDF.filter((F.col("price") > 5000) & ( F.col("review_scores_value")== 10)).agg(F.avg(hotelDF.bathrooms),F.max(hotelDF.bedrooms)).head()
print(avg_bed_bath)
avg_bathrooms = avg_bed_bath[0]
avg_bedrooms = avg_bed_bath[1]
write_file(avg_bathrooms,avg_bedrooms)
