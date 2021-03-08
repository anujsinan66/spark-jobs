from pyspark.sql import SparkSession
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.ml.feature import *
import decimal
import numpy as np
import pandas as pd
import pyspark
import os
import urllib
import sys

def printRecord(record):
  print(record)








def write_file(classes):
  with open("../output/out_3_2.txt", "a") as f:
    for data in classes:
      f.write(str(data)+'\n')
# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('Iris').getOrCreate()

# load iris.csv into Spark dataframe
data = spark.createDataFrame(pd.read_csv('../input/iris.csv', header=None, names=['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']))
print("First 10 rows of Iris dataset:")
data.show(10)

# vectorize all numerical columns into a single feature column
feature_cols = data.columns[:-1]
assembler = pyspark.ml.feature.VectorAssembler(inputCols=feature_cols, outputCol='features')
data = assembler.transform(data)

# convert text labels into indices
data = data.select(['features', 'class'])
label_indexer = pyspark.ml.feature.StringIndexer(inputCol='class', outputCol='label').fit(data)
data = label_indexer.transform(data)

# only select the features and label column
data = data.select(['features', 'label'])
print("Reading for machine learning")
data.show(10)

# change regularization rate and you will likely get a different accuracy.
reg = 0.01
# load regularization rate from argument if present
if len(sys.argv) > 1:
    reg = float(sys.argv[1])


# use Logistic Regression to train on the training set
train, test = data.randomSplit([0.70, 0.30])
lr = pyspark.ml.classification.LogisticRegression(regParam=reg,labelCol="label")
model = lr.fit(train)
# predict on the test set
prediction = model.transform(test)

pred_data =  spark.createDataFrame( [(5.1, 3.5, 1.4, 0.2), (6.2, 3.4, 5.4, 2.3)], ["sepal-length", "sepal-width", "petal-length", "petal-width"])
print(pred_data)
pd1 = assembler.transform(pred_data)
df = model.transform(pd1)
df = df.withColumn("class", expr("case when(prediction='0.0') then 'Iris-setosa' when(prediction='1.0') then 'Iris-versicolor'  else 'Iris-virginica' end"))
class_prediction = df.select("class").rdd.map(lambda x : x['class']).collect()
write_file(class_prediction)

