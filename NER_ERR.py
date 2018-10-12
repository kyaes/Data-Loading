
# coding: utf-8
#spark2-submit  --executor-memory 16g --num-executors 8 --executor-cores 4 --master yarn  /home/liubov/scripts/NER_TO_CSV.py  > /home/liubov/log/NER_TO_CSV.txt 2>&1

import estnltk 
from estnltk import Text as esttext



estnltk.__version__


from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import *

import time
import datetime
from datetime import  timedelta


def remove_names_ee(input_text):
    text = esttext(input_text)
    remove_char_indexes = []

    for named_entity_span in text.named_entity_spans:
        remove_char_indexes.extend(range(named_entity_span[0], named_entity_span[1]))
    input_text = "".join([char for i, char in enumerate(input_text) if i not in remove_char_indexes])
    return input_text 

Same error  appears for lemmatization:


def lemmatizer_ee2(input_text):

    #input_text = str(input_text)
    input_text = unicode(input_text)
    est_text = esttext(input_text)
    #print est_text.lemmas
    lemma = []
    words, types = est_text.get.lemmas.forms.as_list
    for wordtype in zip(words, types):
        if wordtype[0] == 'olema' and wordtype[1] == 'neg o':
            lemma.append('pole')
        elif wordtype[0] == 'ei' and wordtype[1] == 'neg':
            lemma.append('ei')
        else:
            lemma.append(wordtype[0].split('|')[0])
    #lemma = " ".join([ll for ll in lemma if (len(ll) > 2)])
    lemma = " ".join([ll for ll in lemma ])
    return unicode(lemma)



def get_names_ee(input_text):
    text = esttext(input_text)
    return unicode(", ".join(i for i in text.named_entities))


readtable = "Table_name"
writetable=  "Table_name"

now = datetime.datetime.now()
process_run = now.strftime("%Y-%m-%d %H:%M:%S")


#Start Spark2

app_name = "ner-lemma"
from pyspark.sql import SparkSession 
import pyspark as ps
conf = ps.SparkConf().setMaster('yarn-client')
from pyspark import SparkContext, SparkConf
spark = SparkSession.builder.appName(app_name).getOrCreate()

print ("Spark2 started")   



#start sql context
from pyspark.sql import SQLContext, DataFrame
sc = spark.sparkContext
sqlContext = SQLContext(sc)
#start_time = time.time()



#Read data
df = sqlContext.read.format("org.apache.phoenix.spark").option("table", readtable).option("zkUrl", "server1,server2,server3:2181").load()
print("\nTable read done")

df = df.repartition(5000) 

#rem_ner = udf(lambda x:remove_words_shorter_than(replace_ws(replace_ws(replace_ws(remove_names_ee(x).lower(),'stopwords'),'stopwords_en'),'stopverbs'),2))

get_ner = udf(lambda x:get_names_ee(x)) 
#rem_ner = udf(lambda x:remove_names_ee(x).lower())
#lemma = udf(lambda x:lemmatizer_ee2(x)) 




df2 = df.where(col("CLEAN2").isNotNull()).select("ROWKEY", "CLEAN2")
print df2.columns


data_cleaned = df2.withColumn("get_ner", get_ner2(df2.CLEAN2)).withColumn('PROCESS_RUN', lit(process_run).cast(StringType()))
#.withColumn("rem_ner", rem_ner(df2.CLEAN2))
#.withColumn("lemma", rem_ner(df2.rem_ner))


    #Rename columns
ner_data = data_cleaned.select(col("ROWKEY").alias("ROWKEY"), 
                               col("get_ner").alias("NER"),
                               col("PROCESS_RUN").alias("PROCESS_RUN"))
#col("rem_ner").alias("CLEAN4"),
#col("lemma").alias("LEMMA"),
#ner_data = ner_data.repartition(3000)                                        
print "\nRenamed."
print  ner_data.columns

 
ner_data.repartition(5000).write.option("sep","|").option("header","true").csv('/user/liubov/ner_to_csv_out')

'''
# or Insert data to Phoenix Table

ner_data.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", writetable).option("zkUrl", "server1,server2,server3:2181").option("fetchSize","5000").option("numPartitions","5000").save()

print('INSERT COMPLETED')
duration = time.time() - start_time
print("\nDuration: %.3f s" % duration)

'''




sc.stop()
print 'Spark Stopped'


