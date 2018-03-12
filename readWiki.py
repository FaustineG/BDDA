import findspark
findspark.init('/opt/spark')
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

textFile = spark.read.text("wikipedia.dat")

print(textFile.count())
