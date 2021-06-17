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
table = "postgres_landing_member_captured_list"

dymc_frame = glue_context.create_dynamic_frame.from_catalog(database=db, table_name=table)
df_member_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_member').toDF()
df_product_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_product').toDF()
df_broker_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_broker').toDF()
df_client_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_client').toDF()
df_bc_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_broker_client').toDF()

df = dymc_frame.toDF()

# lkp member_id
df = df.join(df_member_lkp, (upper(df.last_name) == df_member_lkp.last_name)&(upper(df.first_name) == df_member_lkp.first_name)&(df.id_number == df_member_lkp.id_number))
# lkp product_id
df = df.join(df_broker_lkp, (df.broker_code == df_broker_lkp.broker_code)&(upper(df.broker)==df_broker_lkp.broker_name))\
    .join(df_client_lkp, (upper(df.client)==df_client_lkp.client_name))
df = df.join(df_bc_lkp, (df.broker_id==df_bc_lkp.broker_id)&(df.client_id==df_bc_lkp.client_id))
df = df.join(df_product_lkp, (df.broker_client_id ==df_product_lkp.broker_client_id)&(df.policy_description == df_product_lkp.policy_description)&(df.cover.cast(IntegerType()) == df_product_lkp.cover)&(df.member_type == df_product_lkp.relate_to))

df_sel = df.select( col("policy_number")
                    , col("member_id")
                    , col("product_id")
                    , to_date(from_unixtime(unix_timestamp("capture_date", "MM/dd/yyyy"), "dd-MM-yyyy"),"dd-MM-yyyy").alias("capture_date")
                    , to_date(from_unixtime(unix_timestamp("inception", "MM/dd/yyyy"), "dd-MM-yyyy"),"dd-MM-yyyy").alias("inception_date")
                    , col("status")
                    , hex(sha1(concat(encode(df.policy_number.cast("string"),"UTF-8"), encode(df.member_id.cast("string"),"UTF-8"), encode(df.product_id.cast("string"),"UTF-8"), encode(df.capture_date.cast("string"),"UTF-8"), encode(df.inception.cast("string"),"UTF-8"), encode(df.status.cast("string"),"UTF-8")))).alias('hash_key')
                    , hex(sha1(concat(encode(df.policy_number.cast("string"),"UTF-8"), encode(df.member_id.cast("string"),"UTF-8"), encode(df.product_id.cast("string"),"UTF-8"), encode(df.capture_date.cast("string"),"UTF-8"), encode(df.inception.cast("string"),"UTF-8"), encode(df.status.cast("string"),"UTF-8")))).alias('hash_diff')
                    , lit('landing_member_captured_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_policy", transformation_ctx = "datasink5")
