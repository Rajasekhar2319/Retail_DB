from pyspark.sql import SparkSession
import os
import logging

spark = SparkSession.builder.appName('sparkExample1.com').getOrCreate()

logging.basicConfig(level="INFO")

# INFO   ---> captures
# Waring  ---> Warnng
# ERROR  ---> Error

src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')
src_file_path_name_ot = os.environ.get('SRC_FILE_PATH_NAME_OT')
formatt = os.environ.get('FORMATT')
envv = os.environ.get('ENVV')


logging.info("Orders data started reading")
if envv == 'DEV':
    df = spark.read.option("header","true").csv(src_file_path_name)
else:
    df = spark.read.format("csv").schema("order_id int , order_date timestamp , order_product_id int , order_status string").load(src_file_path_name)


logging.info("Orders data read completed")

logging.info("Order items data started reading")

df1 = spark.read.format("csv").\
        schema("order_item_id int,order_item_order_id int,order_item_product_id int,order_item_quantity int,order_item_subtotal decimal(17,4),order_item_product_price decimal(17,4)").\
        load(src_file_path_name_ot)


logging.info("Orders items data read completed pushing to feature branch")

df.createOrReplaceTempView("Orders")
df1.createOrReplaceTempView("Order_Items")

df_op = spark.sql("select o.order_date,sum(oi.order_item_subtotal) from Orders o join Order_Items oi on o.order_id = oi.order_item_order_id group by o.order_date where o.order_date between current_date and current_date-1")

df_op.repartition(2).write.format(formatt).save(tgt_file_path_name)