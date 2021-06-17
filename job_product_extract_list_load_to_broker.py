# Import python modules
from datetime import datetime

# Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

# Parameters
s3_write_path = "s3://kgalife/member"
db = "kgalife"
table = "postgres_landing_product_extract_list"

dymc_frame = glue_context.create_dynamic_frame.from_catalog(database=db, table_name=table)

df = dymc_frame.toDF().groupBy("broker_code","broker_name").count()

df_sel = df.select( "broker_code"
                    , upper(col("broker_name")).alias("broker_name")
                    , lit('').alias("broker_branch")
                    , hex(sha1(concat(encode(df.broker_code.cast("string"),"UTF-8"), encode(df.broker_name.cast("string"),"UTF-8")))).alias('hash_key')
                    , hex(sha1(concat(encode(df.broker_code.cast("string"),"UTF-8"), encode(df.broker_name.cast("string"),"UTF-8")))).alias('hash_diff')
                    , lit('landing_product_extract_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_broker", transformation_ctx = "datasink5")
