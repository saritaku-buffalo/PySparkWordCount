from pyspark.context import SparkContext
import string
def countSingle(sc, file):
    lines = sc.textFile(file)
    counts = lines.flatMap(lambda k: k.split(' ')) \
                .map(lambda k: k.lower().translate(str.maketrans('', '', string.punctuation))) \
                .filter(lambda k: k and any(c.isalnum() for c in k) and k not in ["the","and","is","by","that","or","are"]) \
                .map(lambda k: (k, 1)) \
                .reduceByKey(lambda i,j: i+j) \
                .sortBy(lambda k: k[1], False)
    return counts

if __name__ == "__main__":
    
    sc = SparkContext('local', 'appTest')
    counts = countSingle(sc, "The_Blue_Castle.txt")
    counts.sortBy(lambda k: k[1], False).saveAsTextFile("sar")
    sc.stop()