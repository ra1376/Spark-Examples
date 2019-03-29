#******************************************Spark Streaming Program***********************************#
# This program uses london crimes data
#****************************************************************************************************#
#Author: Rahul Ramawat
#This program uses Olympic games dataset available in Kaggle and perform data frame operations on Kaggle
#
#***************************************************************************************************#
from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
	
    sc = SparkContext("local","dfexercise")
	
    sc.setLogLevel("Error")
	
    spark = SparkSession(sc)
	

    athlete_schema = StructType([StructField('id',IntegerType(),True),StructField('name',StringType(),True),StructField('nationality',StringType(),True),\
							 StructField('sex',StringType(),True),StructField('dob',StringType(),True),StructField('height',StringType(),True),\
							 StructField('weight',DecimalType(),True),\
							 StructField('sport',StringType(),True),StructField('gold',IntegerType(),True),StructField('silver',IntegerType(),True),\
							 StructField('bronze',IntegerType(),True)])

'''
    Using class pyspark.sql.DataFrameReader(spark)[source]
    Interface used to load a DataFrame from external storage systems (e.g. file systems, key-value stores, etc). Use spark.read() to access this.
    csv(path, schema=None, sep=None, encoding=None, quote=None, escape=None, comment=None, header=None, inferSchema=None,
	ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None, 
	negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None, maxCharsPerColumn=None,
	maxMalformedLogPerPartition=None, mode=None, columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None, 
	samplingRatio=None, enforceSchema=None, emptyValue=None)[source]
'''
athlete_df = spark.read.csv(path = "C:\\datasets\\Olympic Games\\athletes.csv",sep = ",",schema = athlete_schema,inferSchema = False,header = True)
athlete_df.show(10)

