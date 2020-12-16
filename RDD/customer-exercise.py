from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf=conf)


def parseCustomers(customer):
    fields = customer.split(',')
    a = int(fields[0])
    b = int(fields[1])
    c = int(fields[2])
    return (a, b, c)


customers = sc.textFile(
    "file://Users/rosie/Desktop/sparkcourse/customer-orders.csv")

rdd = customers.map(parseCustomers)
print(rdd)
