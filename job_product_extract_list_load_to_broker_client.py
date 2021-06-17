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
df_broker_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_broker').toDF()
df_client_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_client').toDF()

df = dymc_frame.toDF().groupBy(upper(col("broker_code")).alias("broker_code"), upper(col("broker_name")).alias("broker_name"), upper(col("client")).alias("client")).count()

# lkp member_id
df = df.join(df_broker_lkp, (df.broker_code == df_broker_lkp.broker_code)&(df.broker_name == df_broker_lkp.broker_name)) \
    .join(df_client_lkp, (df.client == df_client_lkp.client_name))

df_sel = df.select(col("broker_id")
                    , col("client_id")
                    , hex(sha1(concat(encode(df.broker_id.cast("string"),"UTF-8"), encode(df.client_id.cast("string"),"UTF-8")))).alias('hash_key')
                    , hex(sha1(concat(encode(df.broker_id.cast("string"),"UTF-8"), encode(df.client_id.cast("string"),"UTF-8")))).alias('hash_diff')
                    , lit('landing_product_extract_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_broker_client", transformation_ctx = "datasink5")
