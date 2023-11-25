# from functools import reduce
#
# d = [1, 2, 3, [5, 6, 7, 8, 9]]
# f = list(filter(lambda x: x > 1, d))
# print(f)
# res = list(map(lambda x: x * x, d))
# print(res)
# r = reduce(lambda a, b: a + b, f)
# print(r)

from session import spark


def takes(rdd, noOfLines):
    i = 0
    for element in rdd.collect():
        print(element)
        i = i + 1
        if i == noOfLines:
            break


data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd = spark.sparkContext.parallelize(data)
# takes(rdd, 100)
rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
# takes(rdd4, 100)
# nested list in pyspark
data = [[[1], [2], [3]], [[4], [5]], [[6]], [[7], [8], [9], [10]]]
rdd5 = spark.sparkContext.parallelize(data)
rdd6 = rdd5.flatMap(lambda xs: [x[0] for x in xs]).take(3)
print(rdd6)
# takes(rdd6, 100)
arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]

# group by key and reduce by key
# initialize the RDD
sc = spark.sparkContext
rdd = sc.parallelize([(u'"COUNTRY"', u'"GYEAR"', u'"PATENT"')
                         , (u'"BE"', u'1963', u'3070801')
                         , (u'"BE"', u'1964', u'3070811')
                         , (u'"US"', u'1963', u'3070802')
                         , (u'"US"', u'1963', u'3070803')
                         , (u'"US"', u'1963', u'3070804')
                         , (u'"US"', u'1963', u'3070805')
                         , (u'"US"', u'1964', u'3070807')])
add = lambda x, y: x + y
rdd_new = rdd.map(lambda x: ((x[0], x[1]), 1)) \
    .reduceByKey(add) \
    .map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
    .reduceByKey(add) \
    .filter(lambda x: x[0] != '"COUNTRY"')
takes(rdd_new, 20)
# [(BE,1963)->1 => BE ->[(1963,1)] =>BE ->[(1963,1),(1964,1)]
# (BE,1964)->1  => BE ->[(1964,1)]
# (US,1963)->4  => US ->[(1963,4)] =>US ->[(1963,4),(1964,1)]
# (US,1964)->1] => US ->[(1964,1)]

# group by example
dataRDD = [("Assignment", 1),
           ("Ruderford", 1),
           ("Manik", 1),
           ("Travelling", 1),
           ("out", 1),
           ("Wonderland", 1),
           ("Assignment", 1),
           ("Ruderford", 1),
           ("Travelling", 1),
           ("in", 1),
           ("Wonderland", 1),
           ("Assignment", 1),
           ("Ruderford", 1)]

# group by Key
rdd = sc.parallelize(dataRDD)
print(rdd.groupByKey().mapValues(len).collect())
print(rdd.groupByKey().mapValues(list).collect())

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
print(sorted(rdd.groupByKey().mapValues(len).collect()))
# [('a', 2), ('b', 1)]
print(sorted(rdd.groupByKey().mapValues(list).collect()))
# [('a', [1, 1]), ('b', [1])]

# reduce by Key
# print(rdd.reduceByKey(lambda x, y: x + y).collect())
# combine By key
x = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])


def to_list(a):
    return [a]


def append(a, b):
    a.append(b)
    return a


def extend(a, b):
    a.extend(b)
    return a


print(sorted(x.combineByKey(to_list, append, extend).collect()))
# [('a', [1, 2]), ('b', [1])]


# Example: Average By Key: use combineByKey()
# https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/combine-by-key/spark-combineByKey.md
data = [("a", 1), ("b", 1), ("a", 1)]
rdd = sc.parallelize(data)


def add(a, b): return a + str(b)


print(sorted(x.combineByKey(str, add, add).collect()))
# Avg
data = [
    ('A', 2.), ('A', 4.), ('A', 9.),
    ('B', 10.), ('B', 20.),
    ('Z', 3.), ('Z', 5.), ('Z', 8.), ('Z', 12.)
]

rdd = sc.parallelize(data)

sumCount = rdd.combineByKey(lambda value: (value, 1),
                            lambda x, value: (x[0] + value, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1])
                            )


def tuple_unpacking(t1, t2):
    x1, y1 = t1
    x2, y2 = t2


# averageByKey = sumCount.map(lambda (key, (totalSum, count)): (key, totalSum / count))
averageByKey = sumCount.map(lambda mk: (mk[0], mk[1][0] / mk[1][1]))

print(averageByKey.collectAsMap())
