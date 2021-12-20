from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

env = os.environ.get('ENVIRON')

src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')
formatt = os.environ.get('FORMATT')
envv = os.environ.get('ENVV')

if envv == 'DEV':
    df = spark.read.option("header","true").csv(src_file_path_name)
else:
    df = spark.read.format("csv").schema("order_id Int , Order_date timestamp, order_product_id int , order_status String").load(src_file_path_name)

df.createOrReplaceTempView("Orders")

df1 = spark.sql("select * from Orders where upper(order_status) in ('COMPLETE','CLOSED') ")

df1.write.format(formatt).option("header","true").mode("overwrite").save(tgt_file_path_name)

### Below is for Client mode #####
#spark-submit --master yarn /home/itv001389/Pycharm_Spark/retail_db_processing.py
#export SRC_FILE_PATH_NAME=hdfs://m01.itversity.com:9000/user/itv001389/retail_db/orders
#export TGT_FILE_PATH_NAME=hdfs://m01.itversity.com:9000/user/itv001389/retail_db/Pycharm_Targets_Processing
#export FORMATT=orc

## For Custer mode it should be as below


#spark-submit \
#--master yarn --deploy-mode cluster \
#--conf "spark.yarn.appMasterEnv.FORMATT=csv" \
#--conf "spark.yarn.appMasterEnv.envv=PROD" \
#--conf "spark.yarn.appMasterEnv.SRC_FILE_PATH_NAME=hdfs://m01.itversity.com:9000/user/itv001389/retail_db/orders" \
#--conf "spark.yarn.appMasterEnv.TGT_FILE_PATH_NAME=hdfs://m01.itversity.com:9000/user/itv001389/retail_db/Pycharm_Targets_Processing" \
#/home/itv001389/Pycharm_Spark/retail_db_processing.py
