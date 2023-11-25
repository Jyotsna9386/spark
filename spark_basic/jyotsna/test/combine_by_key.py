import math

from session import spark

sc = spark.sparkContext

# 0. find Sum of value and how much time key occurs i.e. occurrence count
# 1. find the average of array of tuples
# 2. Mean Calculation by combineByKey()
# 3.Standard Deviation and Mean Calculation by combineByKey()
# 0. Sum count
input = [("k1", 1), ("k1", 2), ("k1", 3), ("k1", 4), ("k1", 5),
         ("k2", 6), ("k2", 7), ("k2", 8),
         ("k3", 10), ("k3", 12)]
rdd = sc.parallelize(input)
sumCount = rdd.combineByKey(lambda x: (x, 1),
                            lambda x, y: (x[0] + y, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1]))
print(sumCount.collectAsMap())
# 1. find the average of array of tuples
sumCount = rdd.combineByKey(lambda x: (x, 1),
                            lambda x, y: (x[0] + y, x[1] + 1),
                            lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg = sumCount.map(lambda x: (x[0], x[1][0] / x[1][1]))
print(avg.collectAsMap())

# 3.Mean and Standard Deviation and Mean Calculation by combineByKey()
data = [
    ('A', 2.0),
    ('A', 4.0),
    ('A', 9.0),
    ('B', 10.0),
    ('B', 20.0),
    ('Z', 3.0),
    ('Z', 5.0),
    ('Z', 8.0),
    ('Z', 12.0)
]
rdd = sc.parallelize(data)
sd = rdd.combineByKey(lambda val: (val, val * val, 1),
                      lambda x, value: (x[0] + value, x[1] + value * value, x[2] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))

print(sd.collect())


def stdDev(sumX, sumSquared, n):
    mean = sumX / n
    stdDeviation = math.sqrt((sumSquared - n * mean * mean) / n)
    return (mean, stdDeviation)


meanAndStdDev = sd.mapValues(lambda x: stdDev(x[0], x[1], x[2]))

# resp = sd.map(lambda val: (val[0], val[1][0] / val[1][2], math.sqrt(val[1][1] / val[1][2])))
print(meanAndStdDev.collect())
