from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf=conf)

input = sc.textFile("book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(word.encode('utf-8', 'ignore'), count)