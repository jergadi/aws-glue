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
df_member_ben_id_lkp = glue_context.create_dynamic_frame.from_catalog(database=db, table_name='postgres_staging_member_beneficiary').toDF()

# deduplicate
df = dymc_frame.toDF().groupBy("first_name","last_name","id_number","beneficiary_full_name_1", "beneficiary_id_number_1", "beneficiary_relation_1","beneficiary_percentage_1","beneficiary_cell_1", "beneficiary_full_name_2", "beneficiary_id_number_2", "beneficiary_relation_2","beneficiary_percentage_2","beneficiary_cell_2", "beneficiary_full_name_3", "beneficiary_id_number_3", "beneficiary_relation_3","beneficiary_percentage_3","beneficiary_cell_3", "beneficiary_full_name_4", "beneficiary_id_number_4", "beneficiary_relation_4","beneficiary_percentage_4","beneficiary_cell_4", "beneficiary_full_name_5", "beneficiary_id_number_5", "beneficiary_relation_5","beneficiary_percentage_5","beneficiary_cell_5").count()

# lkp member_id
df = df.join(df_member_lkp, (df.first_name == df_member_lkp.first_name)&(df.last_name == df_member_lkp.last_name)&(df.id_number == df_member_lkp.id_number)) \
    .select(df_member_lkp.id_number.alias("member_id"),"beneficiary_full_name_1", "beneficiary_id_number_1", "beneficiary_relation_1","beneficiary_percentage_1","beneficiary_cell_1", "beneficiary_full_name_2", "beneficiary_id_number_2", "beneficiary_relation_2","beneficiary_percentage_2","beneficiary_cell_2", "beneficiary_full_name_3", "beneficiary_id_number_3", "beneficiary_relation_3","beneficiary_percentage_3","beneficiary_cell_3", "beneficiary_full_name_4", "beneficiary_id_number_4", "beneficiary_relation_4","beneficiary_percentage_4","beneficiary_cell_4", "beneficiary_full_name_5", "beneficiary_id_number_5", "beneficiary_relation_5","beneficiary_percentage_5","beneficiary_cell_5")

# put into array
df_full_name = df.withColumn("arr_beneficiary_full_name", array(col("beneficiary_full_name_1"), col("beneficiary_full_name_2"), col("beneficiary_full_name_3"), col("beneficiary_full_name_4"), col("beneficiary_full_name_5")))
df_id_number = df.withColumn("arr_beneficiary_id_number", array(col("beneficiary_id_number_1"), col("beneficiary_id_number_2"), col("beneficiary_id_number_3"), col("beneficiary_id_number_4"), col("beneficiary_id_number_5")))
df_relation = df.withColumn("arr_beneficiary_relation", array(col("beneficiary_relation_1"), col("beneficiary_relation_2"), col("beneficiary_relation_3"), col("beneficiary_relation_4"), col("beneficiary_relation_5")))
df_percentage = df.withColumn("arr_beneficiary_percentage", array(col("beneficiary_percentage_1"), col("beneficiary_percentage_2"), col("beneficiary_percentage_3"), col("beneficiary_percentage_4"), col("beneficiary_percentage_5")))
df_cell = df.withColumn("arr_beneficiary_cell", array(col("beneficiary_cell_1"), col("beneficiary_cell_2"), col("beneficiary_cell_3"), col("beneficiary_cell_4"), col("beneficiary_cell_5")))

# explode
df_a = df_full_name.select(col("member_id").alias("a_member_id"), "arr_beneficiary_full_name").withColumn("beneficiary_full_name", explode("arr_beneficiary_full_name")).groupBy("a_member_id","beneficiary_full_name").count().select("a_member_id","beneficiary_full_name")
df_b = df_id_number.select(col("member_id").alias("b_member_id"), "arr_beneficiary_id_number").withColumn("beneficiary_id_number", explode("arr_beneficiary_id_number")).groupBy("b_member_id","beneficiary_id_number").count().select("b_member_id","beneficiary_id_number")
df_c = df_relation.select(col("member_id").alias("c_member_id"), "arr_beneficiary_relation").withColumn("beneficiary_relation", explode("arr_beneficiary_relation")).groupBy("c_member_id","beneficiary_relation").count().select("c_member_id","beneficiary_relation")
df_d = df_percentage.select(col("member_id").alias("d_member_id"), "arr_beneficiary_percentage").withColumn("beneficiary_percentage", explode("arr_beneficiary_percentage")).groupBy("d_member_id","beneficiary_percentage").count().select("d_member_id","beneficiary_percentage")
df_e = df_cell.select(col("member_id").alias("e_member_id"), "arr_beneficiary_cell").withColumn("beneficiary_cell", explode("arr_beneficiary_cell")).groupBy("e_member_id","beneficiary_cell").count().select("e_member_id","beneficiary_cell")

# join all df
df_join = df_a.join(df_b, (df_a.a_member_id == df_b.b_member_id)) \
    .join(df_c, (df_a.a_member_id == df_c.c_member_id)) \
    .join(df_d, (df_a.a_member_id == df_d.d_member_id)) \
    .join(df_e, (df_a.a_member_id == df_e.e_member_id))

df_sel = df_join.select( "member_id"
                    , "beneficiary_full_name", "beneficiary_id_number", "beneficiary_relation",col("beneficiary_percentage").cast(IntegerType()),"beneficiary_cell"
                    , hex(sha1(encode(df_join.a_member_id.cast("string"),"UTF-8"))).alias('hash_key')
                    , hex(sha1(concat(encode(df_join.a_member_id.cast("string"),"UTF-8"), encode(df_join.beneficiary_full_name.cast("string"),"UTF-8"), encode(df_join.beneficiary_id_number.cast("string"),"UTF-8"), encode(df_join.beneficiary_relation.cast("string"),"UTF-8"), encode(df_join.beneficiary_percentage.cast("string"),"UTF-8"),encode(df_join.beneficiary_cell.cast("string"),"UTF-8")))).alias('hash_diff')
                    , lit('landing_member_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_member_beneficiary", transformation_ctx = "datasink5")
