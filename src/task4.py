from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def write_file(min,max,count):
       with open("../output/out_2_2.txt", "a") as f:
                     f.write(str(min)+','+str(max)+','+ str(count))
spark:SparkSession = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()
parDF=spark.read.parquet("../input/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")
min_max = parDF.agg(F.min(parDF.price),F.max(parDF.price),F.count(parDF.price)).head()
col_min = min_max[0]
col_max = min_max[1]
col_count = min_max[2]
write_file(col_min,col_max,col_count)
