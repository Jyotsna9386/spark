from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('06-pyspark-interview').getOrCreate()
data = [('4529', 'Nancy', 'Young', '4125'),
        ('4238', 'John', 'Simon', '4329'),
        ('4329', 'Martina', 'Candreva', '4125'),
        ('4009', 'Klaus', 'Koch', '4329'),
        ('4125', 'Mafalda', 'Ranieri', 'NULL'),
        ('4500', 'Jakub', 'Hrabal', '4529'),
        ('4118', 'Moira', 'Areas', '4952'),
        ('4012', 'Jon', 'Nilssen', '4952'),
        ('4952', 'Sandra', 'Rajkovic', '4529'),
        ('4444', 'Seamus', 'Quinn', '4329')]
import pyspark.sql.functions as F

emp_schema = ['employee_id', 'first_name', 'last_name', 'manager_id']
df = spark.createDataFrame(data, schema=emp_schema)
print(df.show())
df_group = df.groupBy(df["manager_id"]).agg(F.count(df["manager_id"]))
print(df_group.show())
df.createOrReplaceTempView('Emp')
query = '''select  e.manager_id as manager_id, 
count(e.manager_id) as no_of_emp,(m.first_name) as manager_name 
from  Emp e
inner join Emp m 
on e.manager_id =m.employee_id
group by e.manager_id,m.first_name'''
res = spark.sql(query)
print(res.show())
