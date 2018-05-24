
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame

# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')


conf = SparkConf().setAppName("CSV_toPhoenix")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
print "Spark Started"

filedir='hdfs://host_ip:port/user/username/url_data.csv'

read_csv = sc.textFile(filedir)
header = read_csv.first() #find header
data= read_csv.filter(lambda x: x !=header) #remove header

data.take(4) #take sample
data.count() #count rows


from collections import namedtuple
Record = namedtuple("Record", ["code","url"])
def parse_record(row):
    fields = row.split(',')
    return Record( unicode(fields[0]), unicode(fields[1]))
 

parsed_data = data.map(parse_record) #parse fields

df = sqlContext.createDataFrame(parsed_data) #create dataframe

'''
1) Rename columns if needed! - in case HBase wants to get column families.

from pyspark.sql.functions import *
data2 = df.select(col("code").alias("data.code"), 
                     col("url").alias("data.url"))
data2.show(n=2)

2) Create table in Phoenix

CREATE TABLE TEST
(code VARCHAR PRIMARY KEY, 
url VARCHAR);

NB! In TEST table 'code' is the primary key,  duplicates will be removed after inserting

'''


#Write csv data to Phoenix table
df.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", "TEST").option("zkUrl", "localhost:2181").save()


sc.stop()
print "Spark stopped"

