from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import logging

spark = SparkSession.builder.appName('SparkExamples.com').getOrCreate()

env = os.environ.get('ENVIRON')
src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')
src_file_path_name_ot = os.environ.get('SRC_FILE_PATH_NAME_OT')
envv = os.environ.get('ENVV')
formatt = os.environ.get('FORMATT')
logging.basicConfig(level="INFO")

logging.info("orders data reading started")
if envv == 'DEV':
    df = spark.read.option("header","true").csv(src_file_path_name)
else:
    df = spark.read.schema("order_id int,order_date timestamp,order_product_id int,order_status string").format("csv").load(src_file_path_name)
logging.info("orders data reading completed")

logging.info("order_items data reading started")

if envv == 'DEV':
    df1 = spark.read.\
          schema("order_item_id int,order_item_order_id int,order_item_product_id int,order_item_quantity int,order_item_subtotal decimal(17,4),order_item_product_price decimal(17,4)").\
          format("csv").\
          load(src_file_path_name_ot)
else:
    df1 = spark.read.orc(src_file_path_name_ot)
logging.info("order_items data reading completed")

logging.info("orders data processing functions started")

df_11 = df.select("order_id","order_date","order_product_id",initcap("order_status").alias("order_status"))

df_12 = df_11.withColumn("date",substring("order_date",1,10))

df_13 = df_12.where('order_status in ("Complete","Closed")')

df_14 = df_13.filter("order_date like '2013-07%'")

df_15 = df_14.drop("order_date")

logging.info("orders data processing functions completed")


df1_11 = df1.select("order_item_id","order_item_order_id","order_item_quantity","order_item_subtotal","order_item_product_price")

logging.info("Join function started")

df_op = df_15.join(df1_11,df_15.order_id == df1_11.order_item_order_id)

df_op.show()
logging.info("Join function completed")


# Client mode Execution:
#export SRC_FILE_PATH_NAME=hdfs://m01.itversity.com:9000/user/itv001389/retail_db/Pycharm_Targets_Processing_Orders
#export SRC_FILE_PATH_NAME_OT=hdfs://m01.itversity.com:9000/user/itv001389/warehouse/raj.db/order_items_orc
#export TGT_FILE_PATH_NAME=hdfs://m01.itversity.com:9000/user/itv001389/retail_db/Dataframe_Functions
#export FORMATT=csv
#export envv=PROD

#spark-submit --master yarn /home/itv001389/Retail_DB/Dataframe_Functions/Spark_Functions.py

