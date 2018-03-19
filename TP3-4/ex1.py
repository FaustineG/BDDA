import findspark
findspark.init('/opt/spark')

from pyspark import SparkConf, SparkContext

DATA_FILE="onlyBayLocsUsersWithFreq.txt"

def main():
	conf = SparkConf()
	sc = SparkContext(appName="ex1.py", conf=conf)
	statsData(sc)
	dataUser(sc)

def statsData(sc):
	textFile = sc.textFile(DATA_FILE)
	lines = textFile.flatMap(lambda line : line.split("\n"))
	words = lines.map(lambda word : (word, 1))
	print("There is " + str(words.count()) + "  words in this doc")

def dataUser(sc):
	textFile = sc.textFile(DATA_FILE)
	# ligne de la forme user place,nb place,nb
	user = textFile.flatMap(lambda word : word.split(" ")[1:]).map(lambda x: x.split(","))
	print("Nous avons trouve " + str(user.count()) + " endroits")
	places = user.reduceByKey(lambda x,y : int(x) + int(y)).sortBy(lambda x: int(x[1]), ascending = False)
	print("Dont " + str(places.count()) + " endroits differents")
	top5 = places.take(5)
	print("Les 5 plus populaires sont : " + toString(top5))
	
def toString(placesList):
	res = ""
	for((k,v) in placesList):
		res += str(k) + " | vu " + str(v) + " fois \n"
	return res


if __name__ == "__main__":
	main()
