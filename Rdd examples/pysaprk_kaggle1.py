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

sc = pyspark.SparkContext(appName="Pi")
rdd1 = sc.textFile("C:\\KaggleDatasets\\avocado.csv")
first1= rdd1.first()
rdd2 = rdd1.filter(lambda l: l != first1)
print(rdd2.take(2))
rdd3 = rdd2.map(lambda l: l.split(","))
print(rdd3.take(2))
rdd4 = rdd3.map( lambda l: Row(no = int(l[0]),Date = l[1],AveragePrice = float(l[2])
,Total_Volume = float(l[3]),typ4046 = float(l[4]),typ4225 = float(l[5]),typ4770 = float(l[6]),Total_Bags = float(l[7]) ,
Small_Bags = float(l[8]),Large_Bags = float(l[9]),XLarge_Bags = float(l[10]),type = l[11],year = int(l[12]),region = l[13]))
print(rdd4.take(2))
spark = SparkSession(sc)
df = spark.createDataFrame(rdd4)

df.createOrReplaceTempView("avocado")
df1 = spark.sql("select no ,Date ,AveragePrice,Total_Volume,typ4046,typ4225,typ4770,Total_Bags,Small_Bags,Large_Bags,XLarge_Bags,type,year,region from avocado")
df.show(10)
df.printSchema()
df1.write.format("jdbc").option("url","jdbc:mariadb//127.0.0.1:3306/testing").option("driver","mariadb-java-client-2.3.0").option("dbtable","testing.avocado_dtls").option("user","root").option("password","rahul1376").save()

sc.stop()


#C:\KaggleDatasets\pysaprk_kaggle1.py