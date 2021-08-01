from pyspark.sql import SparkSession
spark:SparkSession = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()
csvRdd = spark.sparkContext.textFile("../input/groceries.csv").map(lambda line: line.split(",")).take(5)
for data in csvRdd:
       print(data)
       print('hello')
       print('hello2')
       print('hello6')



