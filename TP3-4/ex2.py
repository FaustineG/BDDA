import findspark
findspark.init('/opt/spark')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# book1.json a book1560.json
DATA_FILE="books-json/*.json"

def main():
	conf = SparkConf()
	sc = SparkContext(appName="ex2.py", conf=conf)
	spark = SparkSession.builder.appName("ex2.py").getOrCreate()
	df = spark.read.json(DATA_FILE, multiLine = True)
	print(df.printSchema())

if __name__ == "__main__":
	main()