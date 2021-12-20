from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

env = os.environ.get('ENVIRON')

src_file_path_name = os.environ.get('SRC_FILE_PATH_NAME')
tgt_file_path_name = os.environ.get('TGT_FILE_PATH_NAME')


df = spark.read.csv(src_file_path_name)

df.printSchema()

df.write.csv(tgt_file_path_name)