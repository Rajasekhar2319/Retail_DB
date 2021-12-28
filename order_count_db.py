from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName('SparkExample.com').getOrCreate()

env = os.environ.get('ENVIRON')

src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')
formatt = os.environ.get('FORMATT')
envv = os.environ.get('ENVV')

if envv == 'DEV':
    df = spark.read.option("header","true").csv(src_file_path_name)
else:

    df = spark.read.format("csv").schema("order_id int , order_date timestamp , order_product_id int , order_status string").load(src_file_path_name)

df.createOrReplaceTempView("orders")

df1 = spark.sql("select Order_date,count(1) from orders group by Order_date")
df1.coalesce(1).write.format('orc').mode("overwrite").save(tgt_file_path_name)

