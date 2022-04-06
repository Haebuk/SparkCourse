from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("totalSpentByCustomer")
sc = SparkContext(conf=conf)

def parseline(text):
    fields = text.split(',')
    id = fields[0]
    cost = fields[2]
    return (int(id), float(cost))

lines = sc.textFile("customer-orders.csv")
totalSpent = lines.map(parseline).reduceByKey(lambda x, y: x + y)
totalSpentSorted = totalSpent.map(lambda x: (x[1], x[0])).sortByKey()
results = totalSpentSorted.collect()

for spent, id in results:
    print(f"{id}: \t {spent:.2f}")