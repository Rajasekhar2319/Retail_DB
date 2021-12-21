from pyspark.sql import SparkSession
import sys
import os
import logging

spark = SparkSession.builder \
    .appName("quickstart_sql") \
    .master("local[*]") \
    .getOrCreate()



logging.basicConfig(level="INFO")



spark.sql("CREATE TABLE raj.Delta_Table_TEST(id LONG) USING delta")
spark.sql("INSERT INTO raj.Delta_Table_TEST VALUES 0, 1, 2, 3, 4")
spark.sql("SELECT * FROM raj.Delta_Table_TEST").show()

logging.info("Initial Delta table created with data")

logging.info("Creating and loading New Delta table")

spark.sql("CREATE TABLE raj.Delta_Table_New_TEST(id LONG) USING parquet")
spark.sql("INSERT INTO raj.Delta_Table_New_TEST VALUES 3, 4, 5, 6")

logging.info("New Delta table created with data")

logging.info("Performing insert overwrite")


spark.sql(
"""MERGE INTO raj.Delta_Table_TEST USING raj.Delta_Table_New_TEST
          ON raj.Delta_Table_TEST.id = raj.Delta_Table_New_TEST.id
          WHEN MATCHED THEN
            UPDATE SET raj.Delta_Table_TEST.id = raj.Delta_Table_New_TEST.id
          WHEN NOT MATCHED THEN INSERT *
      """)

logging.info("Insert overwrite completed")
