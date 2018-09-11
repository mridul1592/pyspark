from pyspark import SparkConf, SparkContext

conf = SparkConf(). \
setAppName("Monthly crime count by type"). \
setMaster("yarn-client")
sc = SparkContext(conf=conf)

def bringDate(crime):
	timestamp = crime[0]
	date = timestamp.split(' ')[0]
	year = date.split('/')[2]
	month = date.split('/')[0]
	return int(year+month)

crimeRawRDD = sc.textFile("/public/crime/csv/crime_data.csv")

crimeRDD = crimeRawRDD.filter(lambda x: 'ID,Case Number' not in x).\
map(lambda y: (y.split(',')[2], y.split(',')[5]))

crimeRecordsWithMonth = crimeRDD.map(lambda c:((bringDate(c), c[1]), 1))

crimeCountByType = crimeRecordsWithMonth.reduceByKey(lambda x, y: x + y)  # get crime count by month and crime

crimeCountByTypeSorted = crimeCountByType.map(lambda r: ((r[0][0], -r[1]), "	".join((str(r[0][0]),str(r[1]),r[0][1])))).sortByKey()
#((200101, -7866), u'200101\t7866\tTHEFT')

crimeCountByTypeSortedFinal = crimeCountByTypeSorted.map(lambda x: (x[1]))

crimeCountByTypeSortedFinal.saveAsTextFile(path = "/user/mridulsharma1592/solutions/solution01/crimes_by_type_by_month", compressionCodecClass = "org.apache.hadoop.io.compress.GzipCodec")