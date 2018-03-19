import findspark
findspark.init('/opt/spark')
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

DATA_FILE = "wikipedia.dat"

def main():
	conf = SparkConf()
	sc = SparkContext(appName="readWiki.py", conf=conf)
	statsData(sc)

def statsData(sc):
	textFile = sc.textFile(DATA_FILE)
	counts = textFile.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b).count()
	print("There is " + str(counts) + " words in this doc")

if __name__ == "__main__":
	main()
