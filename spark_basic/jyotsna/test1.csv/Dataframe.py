from session import spark

df=(spark.read.format('csv')
    .option('path','dept.csv')
    .option('header','True')
    .option('inferSchema','True').load())
print(df.show())
print(df.printSchema())
# df.write.format('csv').save('test1.csv')
# df1=df.select('deptid','deptname','salary')
# print(df1.show())
# df2=df.select(col('deptid'),col('deptname'),col('state'))
# print(df2.show())
# df3=df.selectExpr('deptid','deptname','salary * 10 as TotalSalary')
# print(df3.show())
# from pyspark.sql.functions import expr
# df4=df.select('deptid','deptname',expr('salary * 10 as TotalSalary'))
# print(df4.show())
data = [["1", "sravan", 45000,'001'],
        ["2", "ojaswi", 85000,'002'],
        ["3", "rohith", 41000,'005'],
        ["4", "sridevi",6000,'007'],
        ["5", "bobby", 45000,'005'],
        ["6", "gayatri",49000,'004'],
        ["7", "gnanesh", 5000,'001'],
        ["8", "bhanu", 21000,'008']
        ]
columns = ['EMPID', 'EMPNAME', 'SALARY','DEPTID']

df11 = spark.createDataFrame(data, columns)
print(df11.show())
df12 = df11.withColumn("DEPTID",
                        df11["DEPTID"]
                        .cast('Integer'))
print(df12.printSchema())
# df11.join(df,df11.DEPTID == df.DEPTID,"inner").show()
# df11.groupBy('DEPTID').sum('SALARY').show()
#question 1
df13=df12.join(df,df12.DEPTID == df.DEPTID,"right")
# print(df13.show())
# df14=df13.select('EMPID','EMPNAME','DEPTNAME').filter(df13.DEPTNAME=='Hr')
# print(df14.show())
# Question 2
df15=df12.join(df,df12.DEPTID == df.DEPTID,"left")
print(df15.show())
# df16=df15.agg({'SALARY':'max'})
# print(df16.show())
from pyspark.sql.functions import col
df17=df15.selectExpr('MAX(SALARY) AS MAXSALARY')
print(df17.show())
# df18=df15.selectExpr('EMPID','EMPNAME','DEPTNAME',max('SALARY'))
df19=df15.sort(col('SALARY').desc())
print(df19.show())
df20=df19.select('EMPID','EMPNAME','SALARY','DEPTNAME')
print(df20.show(1))
df21=df13.groupBy('STATE').max('SALARY')
print(df21.show())
df22=df13.groupBy('STATE','EMPID').max('SALARY')
print(df22.show())