from pyspark.sql import SparkSession
import os
import logging

spark = SparkSession.builder.appName('SparkExamples.com').getOrCreate()

env = os.environ.get('ENVIRON')
logging.basicConfig(level='INFO')

src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
src_file_path_name_ot = os.environ.get('SRC_FILE_PATH_NAME_OT')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')
envv = os.environ.get('ENVV')

logging.info("Orders data reading started")
if envv == 'DEV':
    df = spark.read.option("header","true").csv(src_file_path_name)
else:
    df = spark.read.format("csv").schema("order_id int , order_date timestamp , order_product_id int , order_status string").load(src_file_path_name)
logging.info("Orders data reading completed")

df.createOrReplaceTempView("orders")

logging.info("Order_items data reading started")

df1 = spark.read.format("csv").\
          schema("order_item_id int,order_item_order_id int,order_item_product_id int,order_item_quantity int,order_item_subtotal decimal(17,4),order_item_product_price decimal(17,4)").\
          load(src_file_path_name_ot)

df1.createOrReplaceTempView("order_items")
logging.info("Order_items data reading completed")
df2 = spark.sql("select order_item_id,sum(order_item_subtotal) from order_items group by order_item_id order by order_item_id")
df2.coalesce(2).write.option("overwrite").format("csv").save(tgt_file_path_name)




