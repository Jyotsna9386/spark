from session import spark
sc=spark.sparkContext
rdd1=sc.textFile('employee1')
print(rdd1.collect())
print(rdd1.first())
print(rdd1.take(3))
rdd2=rdd1.map(lambda x:x.split(','))
print(rdd2.collect())
print(rdd2.count())
rdd3=rdd1.flatMap(lambda x:x.split(','))
print(rdd3.collect())
print(rdd3.count())
rdd4=rdd3.map(lambda x:(x,1))
print(rdd4.collect())
rdd5=rdd4.reduceByKey(lambda x,y:(x+1))
print(rdd5.collect())
rdd6=rdd5.keys()
print(rdd6.collect())
rdd7=rdd5.values()
print(rdd7.collect())
sort1=rdd5.sortByKey()
print(sort1.collect())
sort2=rdd5.sortByKey(False)
print(sort2.collect())
sort3=rdd5.sortBy(lambda x:x[1])
print(sort3.collect())
sort4=rdd5.sortBy(lambda x:x[1],False)
print(sort4.collect())
new1=rdd5.filter(lambda x:x[1]>1)
print(new1.collect())
new2=rdd5.filter(lambda x:x[1]%2==0)
print(new2.collect())
new3=rdd5.filter(lambda x:x[1]%2==1)
print(new3.collect())
new4=rdd5.filter(lambda x:x[0].startswith('H'))
print(new4.collect())
new5=rdd5.filter(lambda x:x[0].endswith('a'))
print(new5.collect())
new6=rdd5.filter(lambda x:x[0].find('cc')!=-1)
print(new6.collect())