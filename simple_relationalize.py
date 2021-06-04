
#Import python modules
from datetime import datetime

#Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
s3_write_path = "s3://testbucket/test-file"
db = "testdb"
table = "testtable"
glue_temp_storage = "s3://testbucket/temp/glue_temp/lnk_test"
dfc_root_table_name = "root"


dymc_frame = glue_context.create_dynamic_frame.from_catalog(database = db, table_name = table)


pre_dfc = dymc_frame.select_fields(['id', 'bu_id','bp_person_rel_list'])
dfc = Relationalize.apply(frame = pre_dfc, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
rel_list = dfc.select(dfc_root_table_name)
rel_list = rel_list.toDF()

df = dymc_frame.toDF()

df_sel = df.select(hex(sha1(concat(encode(df.id.cast("string"),"UTF-8"), encode(rel_list.trg_obj_id.value.cast("string"),"UTF-8")))).alias('TEST_PERSON_HK') 
, hex(sha1(encode(df.id.cast("string"), "UTF-8"))).alias('HUB_BP_HK')
, hex(sha1(encode(coalesce(rel_list.trg_obj_id.value, lit(0)).cast("string"), "UTF-8"))).alias('HUB_PERSON_HK')
, lit('ACP').alias('LNK_REC_SRC')
, df.bu_id.value.alias('bu_id')).withColumn("LOAD_DTS", current_timestamp())

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },
    format = "parquet"
)
