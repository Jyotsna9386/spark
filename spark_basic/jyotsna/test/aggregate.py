from session import spark

sc = spark.sparkContext

listRDD = sc.parallelize([1, 2, 3, 4], 2)
seqOp = (lambda local_result, list_element: (local_result[0] + list_element, local_result[1] + 1))
combOp = (lambda some_local_result, another_local_result: (
    some_local_result[0] + another_local_result[0], some_local_result[1] + another_local_result[1]))

res = listRDD.aggregate((0, 0), seqOp, combOp)
print(res)
