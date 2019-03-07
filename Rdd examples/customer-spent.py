#****************************************TotalCustomerSpent************************************#
#this program illustrates how the customer spent on a fake ecommerce data
#Author: Rahul Ramawat
#############################################################################################

#Importing Library
from pyspark import SparkContext
from operator import add

#Importing SparkContext
sc = SparkContext(master = "local[*]",appName = "CustomerSpent")
sc.setLogLevel("Error") #setting error log
'''
'''
#** function to parse 
def parse_func(lines):
	fields = lines.split(",")
	custid = fields[0]
	amountspent = float(fields[2])
	return (custid,amountspent)

''' 
Main program starts
'''
input = sc.textFile("C:\\Python_Scripts\\customer-orders.csv")  # converting any sample data to RDD

mappedInput = input.map(parse_func)
    
totalByCustomer = mappedInput.reduceByKey(add)

results = totalByCustomer.collect()

print(results)
for value in results:
	print("Customer {} spent {} dollars.\n".format(value[0],value[1]))

sc.stop()
	