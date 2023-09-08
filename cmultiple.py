import os
from pyspark.context import SparkContext
import string
def countMultiple(sc, files):
    count = None
    for file in files:
        lines = sc.textFile(file) 
        res = lines.flatMap(lambda k: k.split(' ')) \
                .map(lambda k: k.lower().translate(str.maketrans('', '', string.punctuation))) \
                .filter(lambda k: k and any(c.isalnum() for c in k) and k not in ["the","and","is","by","that","or","are"]) \
                .map(lambda k: (k, 1)) \
                

        if count == None:
            count = res
        else:
            count = count.union(res)
    return count.reduceByKey(lambda i,j: i+j) \

if __name__ == '__main__':
    sc = SparkContext('local', 'multipleText')

    res = countMultiple(sc, ["Adventures_in_Wonderland.txt","Cranford.txt","Frankenstein.txt","My_Life.txt","Pride_and_prejudice.txt","Room_With_View.txt","The_Adventures_of_Roderick_Random.txt","The_Blue_Castle.txt","The_Great_Gatsby.txt","Twenty_Years_After.txt"])

    res.coalesce(1) \
        .sortBy(lambda k: k[1], False).saveAsTextFile("sar")

