#ETL using Spark
# To MariaDB
# Create Schema using 
import pyspark
from pyspark.sql import SparkSession 
#from pyspark 
from pyspark.sql import Row as Row
from pyspark.sql.types import *
from pyspark.sql import functions as f
import datetime as dt

#declaring schema for the dataframe
'''schema = StructType([StructField("no",IntegerType(),True),\
					StructField("date",StringType(),True),\
					StructField("avgprice",DecimalType(),True),\
					StructField("totvolume",DecimalType(),True),\
					StructField("typ4046",DecimalType(),True),\
					StructField("typ4225",DecimalType(),True),\
					StructField("typ4770",DecimalType(),True),\
					StructField("tot_bags",DecimalType(),True),\
					StructField("small_bags",DecimalType(),True),\
					StructField("large_bags",DecimalType(),True),\
					StructField("xl_bags",DecimalType(),True),\
					StructField("type",StringType(),True),\
					StructField("year",IntegerType(),True),\
					StructField("region",IntegerType(),True)])

print(schema)'''					
'''SparkContext for the application'''
sc = pyspark.SparkContext(master = "local[*]",appName="Pi")
rdd1 = sc.textFile("C:\\KaggleDatasets\\avocado.csv") #Any csv file with or without header I am using Avocado dataset available in Kaggle
first1= rdd1.first()
rddfilt = rdd1.filter(lambda l: l != first1) #removing the header line of csv file converted to rdd
rddsplit = rddfilt.map(lambda l: l.split(","))

rddrow = rddsplit.map( lambda l: Row(l[0],l[1],l[2],l[3],l[4],l[5],l[6],l[7],\
							l[8],l[9],l[10],l[11],l[12],l[13]))

spark = SparkSession(sc)	
df = spark.createDataFrame(rddrow) # without schema
df.printSchema()
df.show(10)
df.createOrReplaceTempView("avocado_dtls")
dfsql = spark.sql("select * from avocado_dtls limit 10") # selecting all columns
dfsql.show(10)



sc.stop()
