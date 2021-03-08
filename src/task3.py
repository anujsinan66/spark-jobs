from pyspark.sql import SparkSession

def write_file(product_list):
       with open("../output/out_1_3.txt", "a") as f:
              for data in product_list:
                     f.write(str(data)+'\n')
spark:SparkSession = SparkSession.builder.master("local[*]").appName("task1").getOrCreate()
products = spark.sparkContext.textFile("../input/groceries.csv").flatMap(lambda line: line.split(","))
productsCounts = products.map(lambda product: (product, 1)).reduceByKey(lambda a,b:a +b)
product_list = productsCounts.map(lambda x: (x[1], x[0])).sortByKey(False).take(5)
write_file(product_list)





