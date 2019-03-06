#****************************************MAX Temperature************************************#
#this program illustrates how you can find a maximum value in RDD
#Author: Rahul Ramawat
#############################################################################################

#Importing Library
from pyspark import SparkContext
#from operator import max

#Importing SparkContext
sc = SparkContext(master = "local[*]",appName = "MaxTemperatures")
sc.setLogLevel("Error") #setting error log
'''
'''
#** function to parse 
def parse_func(lines):
	fields = lines.split(",")
	stationid = fields[0]
	entrytype = fields[2]
	temperature = float(fields[3])*float(0.1)*(float(9.0)/float(5.0)) + float(32.0)
	return (stationid,entrytype,temperature)

'''
Main program starts
'''
	
lines = sc.textFile("C:\\Python_Scripts\\1800.csv")  #Generic files
parsedlines = lines.map(parse_func)
maxtemps = parsedlines.filter(lambda l : l[1] == "TMAX")
stationtemps = maxtemps.map(lambda l: (l[0],float(l[2])))
maxbystationtemp = stationtemps.reduceByKey(max)
results = (maxbystationtemp.collect())[0]
print(type(results))
print("The station {} has max temp of {}".format(results[0],results[1]))
print(parsedlines.take(3))
print(maxtemps.take(4))
print(stationtemps.take(4))
print(results)
