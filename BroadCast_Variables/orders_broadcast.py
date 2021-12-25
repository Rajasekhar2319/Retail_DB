from pyspark.sql import SparkSession
import os
import sys
import logging

spark = SparkSession.builder.appName('SparkExamples1.com'). \
        config("spark.sql.autoBroadcastJoinThreshold","192093039") . \
        getOrCreate()

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold","192093039")


src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')
src_file_path_name_ot = os.environ.get('SRC_FILE_PATH_NAME_OT')
logging.basicConfig(level="INFO")
envv = os.environ.get('ENVV')
formatt = os.environ.get('FORMATT')

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",1084514)

logging.info("orders data reading started")
if envv == 'DEV':
    df = spark.read.option("header","true").csv(src_file_path_name)
else:
    df = spark.read.schema("order_id int,order_date timestamp,order_product_id int,order_status string").format("csv").load(src_file_path_name)
logging.info("orders data reading completed")


logging.info("orderItems data reading started")
if envv == 'DEV':
    df1 = spark.read.\
          schema("order_item_id int,order_item_order_id int,order_item_product_id int,order_item_quantity int,order_item_subtotal decimal(17,4),order_item_product_price decimal(17,4)").\
          format("csv").\
          load(src_file_path_name_ot)
else:
    df1 = spark.read.option("header","true").orc(src_file_path_name_ot)
logging.info("orderItems data reading completed")

df.createOrReplaceTempView("orders")
df1.createOrReplaceTempView("orderItems")

logging.info("Data processing started")
df_op = spark.sql("select o.order_date,o.order_status,sum(oi.order_item_subtotal) total_rev from orders o join orderItems oi on o.order_id=oi.order_item_order_id group by o.order_date,o.order_status order by o.order_date")

df_op.repartition(2).write.mode("overwrite").format("csv").save(tgt_file_path_name)

logging.info("Data Processing completed")
