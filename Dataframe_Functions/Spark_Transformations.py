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
    order = spark.read.option("header","true").csv(src_file_path_name)
else:
    order = spark.read.format("csv").schema("order_id int , order_date timestamp , order_product_id int , order_status string").load(src_file_path_name)
logging.info("orders data reading completed")

logging.info("order_items data reading started")

if envv == 'DEV':
    orderItem = spark.read.\
          schema("order_item_id int,order_item_order_id int,order_item_product_id int,order_item_quantity int,order_item_subtotal decimal(17,4),order_item_product_price decimal(17,4)").\
          format("csv").\
          load(src_file_path_name_ot)
else:
    orderItem = spark.read.\
          schema("order_item_id int,order_item_order_id int,order_item_product_id int,order_item_quantity int,order_item_subtotal decimal(17,4),order_item_product_price decimal(17,4)").\
          format("csv").\
          load(src_file_path_name_ot)
logging.info("order_items data reading completed")

df = orderItem.where('order_item_subtotal != round(order_item_quantity * order_item_product_price, 2)')
df1 = order.where("order_status in ('COMPLETE','CLOSED')")

df2 = order.\
      join(orderItem, order.order_id == orderItem.order_item_order_id,'left').\
      where("order_item_order_id is null").\
      select("order_id","order_status","order_date","order_item_product_id")

df3 = order.\
      join(orderItem, order.order_id == orderItem.order_item_order_id)


df4 = df3.groupBy('order_item_order_id').\
      agg(round(sum('order_item_subtotal'), 2).alias('order_revenue'))

df5 = df1.\
      join(orderItem, order.order_id == orderItem.order_item_order_id).\
      groupBy('order_date', 'order_item_product_id').\
      agg(round(sum('order_item_subtotal'), 2).alias('revenue'))

logging.info("Final processing started")

df6 = df5.orderBy("order_date","order_item_product_id")

df6.show()

logging.info("Final processing completed")

df6.write.mode("overwrite").format("csv").save('tgt_file_path_name')



