from __future__ import print_function

import sys

from pyspark import SparkContext

from pyspark.sql.types import *

from pyspark.sql import SparkSession

 

if __name__ == "__main__":
	
	sc = SparkContext("local","dfexercise")

	session = SparkSession(sc)
	schema = StructType([StructField("month",StringType(),True),StructField("reportedby",StringType(),True),StructField("crime",StringType(),True),StructField("category",StringType(),True)])

	fileStreamDF = session.readStream\
	.option("header","true")\
	.schema(schema)\
	.csv("C:\\Python_Scripts\\streamingdataset")

               

	print(" ")

	print("is the stream ready ?")

	print(fileStreamDF.isStreaming)

	trimmedDF = fileStreamDF.select(fileStreamDF.month,fileStreamDF.reportedby,fileStreamDF.crime,fileStreamDF.category).withColumnRenamed("category","newcat")

	query = trimmedDF.writeStream.outputMode("append")\
	.format("console")\
	.option("truncate","false")\
	.option("numRows",30)\
	.start()\
	.awaitTermination()