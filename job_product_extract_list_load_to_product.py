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
df = dymc_frame.toDF().groupBy("policy_code" , "policy_description" , "relate_to", "age_from" , "age_to" , "premium" , "underwriter_premium" , "cover" , "waiting_period" , "product_category" , "product_type" , "broker_code" , "broker_name" , "client" , "underwriter" ).count()

df_broker_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_broker').toDF()
df_client_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_client').toDF()
df_bc_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_broker_client').toDF()

# lkp broker_client_id
df = df.join(df_broker_lkp, (df.broker_code == df_broker_lkp.broker_code)&(df.broker_name == df_broker_lkp.broker_name)) \
    .join(df_client_lkp, (df.client == df_client_lkp.client_name))

df = df.join(df_bc_lkp, (df.broker_id == df_bc_lkp.broker_id)&(df.client_id == df_bc_lkp.client_id))

df_sel = df.select( col("policy_code")
                    , col("policy_description")
                    , col("broker_client_id")
                    , "relate_to"
                    , col("premium").alias("retail_premium")
                    , "underwriter"
                    , col("age_to").cast(IntegerType())
                    , col("age_from").cast(IntegerType())
                    , col("underwriter_premium").cast(DecimalType(7,2))
                    , col("cover").cast(IntegerType())
                    , col("waiting_period").cast(IntegerType())
                    , "product_category"
                    , "product_type"
                    , lit('').alias("grace_period")
                    , lit(0).alias("number_of_dependents")
                    , hex(sha1(concat(encode(df.policy_code.cast("string"),"UTF-8"), encode(df.policy_description.cast("string"),"UTF-8"), encode(df.broker_client_id.cast("string"),"UTF-8"), encode(df.relate_to.cast("string"),"UTF-8"), encode(df.age_from.cast("string"),"UTF-8"), encode(df.age_to.cast("string"),"UTF-8")))).alias('hash_key')
                    , hex(sha1(concat(encode(df.policy_code.cast("string"),"UTF-8"), encode(df.policy_description.cast("string"),"UTF-8"), encode(df.broker_client_id.cast("string"),"UTF-8"), encode(df.relate_to.cast("string"),"UTF-8"), encode(df.age_from.cast("string"),"UTF-8"), encode(df.age_to.cast("string"),"UTF-8")))).alias('hash_diff')
                    , lit('landing_product_extract_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_product", transformation_ctx = "datasink5")
