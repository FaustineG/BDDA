import findspark
findspark.init('/opt/spark')

from pyspark import SparkConf, SparkContext

DATA_FILE="onlyBayLocsUsersWithFreq.txt"