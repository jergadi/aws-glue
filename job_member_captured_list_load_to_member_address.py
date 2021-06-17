# Import python modules
from datetime import datetime

# Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
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
df_member_add_id_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_member_address').toDF()

# deduplicate
df = dymc_frame.toDF().groupBy("first_name","last_name","id_number","postal_add_1" , "postal_add_2" , "postal_add_3" , "postal_code" , "res_address_1" , "res_address_2" , "res_address_3" , "res_code").count()

# lkp member_id
df = df.join(df_member_lkp, (df.first_name == df_member_lkp.first_name)&(df.last_name == df_member_lkp.last_name)&(df.id_number == df_member_lkp.id_number)) \
    .select(df_member_lkp.id_number.alias("member_id"),"postal_add_1" , "postal_add_2" , "postal_add_3" , "postal_code" , "res_address_1" , "res_address_2" , "res_address_3" , "res_code")


df_sel = df.select( "member_id"
                    , "postal_add_1" , "postal_add_2" , "postal_add_3" , "postal_code" , "res_address_1" , "res_address_2" , "res_address_3" , "res_code"
                    , hex(sha1(concat(encode(df.id_number.cast("string"),"UTF-8"), encode(df.first_name.cast("string"),"UTF-8"), encode(df.last_name.cast("string"),"UTF-8")))).alias('hash_key')
                    , hex(sha1(concat(encode(df.id_number.cast("string"),"UTF-8"), encode(df.first_name.cast("string"),"UTF-8"), encode(df.last_name.cast("string"),"UTF-8"), encode(df.gender.cast("string"),"UTF-8"), encode(df.date_of_birth.cast("string"),"UTF-8") ))).alias('hash_diff')
                    , lit('landing_member_captured_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))


dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_member", transformation_ctx = "datasink5")
