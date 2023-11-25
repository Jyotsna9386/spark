from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('03-pyspark-interview').getOrCreate()
checkin_df = spark.createDataFrame([(1000, 'login', '2023-06-16 01:00:15.34'),
                                    (1000, 'login', '2023-06-16 02:00:15.34'),
                                    (1000, 'login', '2023-06-16 03:00:15.34'),
                                    (1000, 'logout', '2023-06-16 12:00:15.34'),
                                    (1001, 'login', '2023-06-16 01:00:15.34'),
                                    (1001, 'login', '2023-06-16 02:00:15.34'),
                                    (1001, 'login', '2023-06-16 03:00:15.34'),
                                    (1001, 'logout', '2023-06-16 12:00:15.34')],
                                   ["employeeid", "entry_details", "timestamp_details"])
print(checkin_df.show(truncate=False))
detail_df = spark.createDataFrame([(1001, 9999, 'false'),
                                   (1001, 1111, 'false'),
                                   (1001, 2222, 'true'),
                                   (1003, 3333, 'false')],
                                  ["id", "phone_number", "isdefault"])
print(detail_df.show(truncate=False))
joined_df=checkin_df.join(detail_df,checkin_df['employeeid']==detail_df['id'],'left')
print(joined_df.show(truncate=False))
joined_df=joined_df.where(detail_df['isdefault']=='true')
print(joined_df.show(truncate=False))
# or
# joined_df=checkin_df.join(detail_df,checkin_df['employeeid']==detail_df['id'],'left').where(detail_df['isdefault']=='true')
# print(joined_df.show(truncate=False))
total_entry_df=checkin_df.groupBy(checkin_df['employeeid']).agg(F.count('*').alias('Total_Entry'))
print(total_entry_df.show())
total_login_df=checkin_df.filter(checkin_df['entry_details']=='login').groupBy(checkin_df['employeeid']).agg(F.count('*').alias('Total_login'))
latest_login_df=checkin_df.filter(checkin_df['entry_details']=='login').orderBy(checkin_df['timestamp_details'].desc()).groupBy(checkin_df['employeeid']).agg(F.first('timestamp_details').alias('Latest_login'))
print(latest_login_df.show())
latest_logout_df=checkin_df.filter(checkin_df['entry_details']=='logout').orderBy(checkin_df['timestamp_details'].desc()).groupBy(checkin_df['employeeid']).agg(F.first('timestamp_details').alias('Latest_logout'))
print(latest_logout_df.show())
final_entry=total_entry_df.join(total_login_df,on='employeeid',how='inner')
final_entry=final_entry.join(latest_login_df,on='employeeid',how='inner')
final_entry=final_entry.join(latest_logout_df,on='employeeid',how='inner')
print(final_entry.show(truncate=False))



