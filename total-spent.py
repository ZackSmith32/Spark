from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('users-total-spend')
sc = SparkContext(conf = conf)

def parseUsers(line):
    cont = line.split(',')
    userID = int(cont[0])
    spent = float(cont[2])
    return (userID, spent)
    
lines = sc.textFile('./customer-orders.csv')
rdd = lines.map(parseUsers)
totalsByUser = rdd.reduceByKey(lambda x, y: x + y)
totalsByUserSorted = totalsByUser.map(lambda x: (x[1], x[0])).sortByKey()
results = totalsByUserSorted.collect()

for result in results:
    print("{}\t{:.2f}".format(result[1], result[0]))
