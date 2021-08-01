from pyspark.sql import SparkSession


def write_file(count):
       with open("../output/out_1_2b.txt", "a") as f:
              f.write("Count:\n")
              f.write(str(count))
spark:SparkSession = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()
csvRdd = spark.sparkContext.textFile("../input/groceries.csv").flatMap(lambda line: line.split(",")).distinct()
write_file(csvRdd.count())
print('hello')



