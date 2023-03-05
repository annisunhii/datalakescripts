#aws glue job to load new files into the redshift_client_submitted_data table and update uid based on fuzzy match rules
from multiprocessing import Condition
import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import split,upper, col, trim, date_format, to_date, regexp_replace
from pyspark.sql.types import StructType, StringType, StructField, \
    IntegerType, TimestampType, DateType
from pyspark.sql import Window
from awsglue.transforms import ResolveChoice,DropNullFields
from dateutil.relativedelta import relativedelta



args = getResolvedOptions(sys.argv, ["JOB_NAME","ph_environment","TempDir", "region"])
environment = args["ph_environment"]
region = args["region"]
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
max_stage_errors = 100
max_total_errors = 250
temp_dir = args["TempDir"]
database = "ph_"+environment+"_db_data_lake"
current_timestamp = datetime.utcnow()
table = "dim_member_employer_dignity"
columns_to_drop =["uid_dim_user", "date_of_birth_dim_user","first_name_dim_user","last_name_dim_user","promo_dim_user"]


client_submitted_data_schema = StructType([
StructField("uid",                              
                StringType(),       True),
StructField("promo",                           
                StringType(),       True),
StructField("date_ingested",                       
                DateType(),       False),                                             
StructField("client_name",
                StringType(),       True),                                            
StructField("first_name",
                StringType(),       True),                                           
StructField("last_name",
                StringType(),       True),                                           
StructField("email",
                StringType(),       True),                                                
StructField("address_street_1",
                StringType(),       True),                                  
StructField("address_street_2",
                StringType(),       True),                                   
StructField("address_city",
                StringType(),       True),                                  
StructField("address_state_code",
                StringType(),       True),                                    
StructField("address_postal_code",
                StringType(),       True),                                      
StructField("phone_number",
                StringType(),       True),                                          
StructField("disease",
                StringType(),       True),                                                
StructField("division",
                StringType(),       True),                                               
StructField("market",
                StringType(),       True),                                               
StructField("medical_record_number",
                StringType(),       True),                                   
StructField("patient_gender",
                StringType(),       True),                                       
StructField("patient_language",
                StringType(),       True),                                       
StructField("date_of_birth",                       
                DateType(),        True),                                             
StructField("date_of_service",                       
                DateType(),        True),                                                
StructField("diagnosis_code_1",
                StringType(),       True),                                         
StructField("diagnosis_desc_1",
                StringType(),       True),                                     
StructField("rendering_provider",
                StringType(),       True),                              
StructField("pcp_name",
                StringType(),       True),                                        
StructField("clinic_desc",
                StringType(),       True),                                      
StructField("datetime_modified",
                TimestampType(),    False),   
StructField("cerner_person_id",
                IntegerType(),       True), 
StructField("group_display_name",
                StringType(),       True),    
StructField("address_country_code",
                StringType(),       True),                                  

])


def map_to_dict(row):
    data = row.asDict()
    return data

def fuzzy_match_cond(df, dim_user_df):
    cond =[F.levenshtein(df["first_name"], dim_user_df["first_name_dim_user"]) < 1, 
            F.levenshtein(df["last_name"], dim_user_df["last_name_dim_user"]) < 1]
    joined_rdd= df.join(dim_user_df, cond).rdd.map(map_to_dict)
    unmatched_rdd= df.join(dim_user_df, cond, "left_anti").rdd.map(map_to_dict)
    return joined_rdd, unmatched_rdd

def split_date_client(t):
    disease= ["ASTHMA", "copd", "Asthma"]
    if len(t["date_of_birth"])<1:
        t["date_of_birth"]=None
    elif len(t["date_of_service"])<1:
        t["date_of_service"]=None
    else:
        #input is a string, could be 09/27/01 or 9/27/2001
        if t["disease"].strip()=="COPD":
            t["date_of_birth"]=datetime.strptime(t["date_of_birth"], "%m/%d/%Y").date()
            t["date_of_service"]=datetime.strptime(t["date_of_service"], "%m/%d/%Y").date()
        elif t["disease"].strip() in disease:
            t["date_of_birth"]= datetime.strptime(t["date_of_birth"], "%m/%d/%y").date()
            t["date_of_service"]= datetime.strptime(t["date_of_service"], "%m/%d/%y").date()

    return t


def df_prep():
    client_submitted_df_v1 = glue_context.create_dynamic_frame.from_catalog(
                            database=database,
                            table_name="dignity_dignity")\
                                .toDF()

    client_submitted_df_v1=client_submitted_df_v1.withColumn("cerner_person_id", F.lit("").cast("int"))\
                                                    .withColumn("group_display_name", F.lit("").cast("string"))\
                                                        .withColumn("address_country_code",F.lit("").cast("string"))
                                                            
    client_submitted_df_v2 = glue_context.create_dynamic_frame.from_catalog(
                        database=database,
                        table_name="dignity_dignity_v2")\
                            .toDF()
    
    client_submitted_df_v2 =client_submitted_df_v2.withColumn("diagnosis", regexp_replace('diagnosis', 'COPD', 'copd'))

    client_submitted_df=client_submitted_df_v1.unionByName(client_submitted_df_v2)\
                            .select(
                                    trim(col("zip").cast("string")).alias("address_postal_code"),
                                    trim(col("address-1").cast("string")).alias("address_street_1"),
                                    trim(col("address-2").cast("string")).alias("address_street_2"),
                                    trim(col("city").cast("string")).alias("address_city"),
                                    trim(col("diagnosis").cast("string")).alias("disease"),
                                    trim(col("email").cast("string")).alias("email"),
                                    trim(col("mem_first_name").cast("string")).alias("first_name"),
                                    trim(col("mem_last_name").cast("string")).alias("last_name"),
                                    trim(col("phone nbr").cast("string")).alias("phone_number"),
                                    trim(col("state").cast("string")).alias("address_state_code"),
                                    F.lit("").alias("uid"),
                                    F.lit("").alias("promo"),
                                    trim(col("client name").cast("string")).alias("client_name"),
                                    trim(col("division").cast("string")).alias("division"),
                                    trim(col("market").cast("string")).alias("market"),
                                    trim(col("medical_record_number").cast("string")).alias("medical_record_number"),
                                    trim(col("patient_gender").cast("string")).alias("patient_gender"),
                                    trim(col("patient_language").cast("string")).alias("patient_language"),
                                    trim(col("patient_date_of_birth").cast("string")).alias("date_of_birth"),
                                    trim(col("date_of_service").cast("string")).alias("date_of_service"),
                                    trim(col("diagnosis_code_1").cast("string")).alias("diagnosis_code_1"),
                                    trim(col("diagnosis_desc_1").cast("string")).alias("diagnosis_desc_1"),
                                    trim(col("rendering_provider").cast("string")).alias("rendering_provider"),
                                    trim(col("pcp_name").cast("string")).alias("pcp_name"),
                                    trim(col("clinic_desc").cast("string")).alias("clinic_desc"),
                                    F.lit(current_timestamp).cast("timestamp").alias("datetime_modified"),
                                    trim(col("date_ingested")).cast("date").alias("date_ingested"),
                                    trim(col("cerner_person_id")).cast("int").alias("cerner_person_id"),
                                    trim(col("group_display_name").cast("string")).alias("group_display_name"),
                                    trim(col("address_country_code").cast("string")).alias("address_country_code")

                                )
    # get the df_dim_user, get the connection from redshift connection
    dim_user_df = glue_context.create_dynamic_frame.from_catalog(
        database= database,
        table_name="ph_analytics_dim_user",
        redshift_tmp_dir=temp_dir).toDF()

    #match the format in client data (they are all uppercase)
    dim_user_df =  dim_user_df.select(
                    dim_user_df.uid.cast("string").alias("uid_dim_user"),
                    F.col("date_of_birth").alias("date_of_birth_dim_user"),
                    dim_user_df.promo.cast("string").alias("promo_dim_user"),
                    upper(trim(col("first_name"))).cast("string").alias("first_name_dim_user"),
                    upper(trim(col("last_name"))).cast("string").alias("last_name_dim_user")
                )

    return client_submitted_df, dim_user_df

#make sure exact match on age and zip scode, and if true, replace the df1.uid with dim_user_df.uid, drop all the dim_user_df columns
def update_uid_promo(t):
    promo_list_dignity=["dhdominicanpulm", "dhfairoaks", "dhinlandempire", "dhlongbeach", "dhmidtownallergy", "dhphoenix", "dhventura", "woodland"]
    if t["date_of_birth"] and t["date_of_birth_dim_user"]:
        if (t["date_of_birth"] == t["date_of_birth_dim_user"]) & (t["promo_dim_user"] in promo_list_dignity):
            t["uid"] = t["uid_dim_user"]
            t["promo"] = t["promo_dim_user"]
        else: 
            t["uid"] = None
            t["promo"]= None
    else: 
        t["uid"] = None
        t["promo"]= None

    #drop the columns from dim_user   
    for column in columns_to_drop: 
        if t[column]:
            del t[column]

    return t

def update_unmatched(t):
    t["uid"] = None
    t["promo"]= None
    return t


def get_resolve_choice_specs_from_schema(schema):
    type_map = {
        "BooleanType":      "cast:boolean",
        "DateType":         "cast:date",
        "DoubleType":       "cast:float",
        "IntegerType":      "cast:int",
        "LongType":         "cast:int",
        "StringType":       "cast:string",
        "TimestampType":    "cast:timestamp"
    }

    return [(col.name, type_map[str(col.dataType)]) for col in schema]

def dob_not_exceeds_2022(t):
    #dob field is already a date type from above transformation
    present = datetime.now()
    if t["date_of_birth"] > present.date():
        t["date_of_birth"]=t["date_of_birth"] - relativedelta(years=100)
    return t

    
def main():  

    client_submitted_df, dim_user_df = df_prep()
    joined_rdd, unmatched_rdd= fuzzy_match_cond(client_submitted_df, dim_user_df)
    joined_rdd= joined_rdd\
                    .map(split_date_client)\
                    .map(dob_not_exceeds_2022)\
                    .map(update_uid_promo)
    unmatched_rdd=unmatched_rdd.map(update_unmatched)\
                         .map(split_date_client)\
                        .map(dob_not_exceeds_2022)

    total_rdd= joined_rdd.union(unmatched_rdd)

    updated_df = spark.createDataFrame(
            total_rdd,
            schema=client_submitted_data_schema
    )
    
    #possible that same person have both asthma and copd
    name_window= Window \
        .partitionBy("client_name","first_name","last_name","email","address_street_1","address_street_2","address_city","address_state_code","address_postal_code","phone_number","disease","division","market","medical_record_number","patient_gender","patient_language","date_of_birth","date_of_service","diagnosis_code_1","diagnosis_desc_1","rendering_provider","pcp_name","clinic_desc","datetime_modified","cerner_person_id","group_display_name","address_country_code","date_ingested") \
        .orderBy(F.desc("uid"))
    updated_df = updated_df.withColumn("row_number", F.row_number().over(name_window))
    new_rdd= updated_df.filter(updated_df.row_number == 1)\
            .drop("row_number")\
            .rdd  
       
    updated_df = spark.createDataFrame(
        new_rdd,
        schema=client_submitted_data_schema
    )

    updated_dyf = DynamicFrame.fromDF(
        dataframe=updated_df.dropDuplicates(),
        glue_ctx=glue_context,
        name="updated_df"
    )

    updated_dyf = ResolveChoice.apply(
    frame=updated_dyf,
    stageThreshold=max_stage_errors,
    totalThreshold=max_total_errors,
    specs=get_resolve_choice_specs_from_schema(client_submitted_data_schema)
    )
    
    stage_table = "analytics.stage_" + table
    target_table = "analytics." + table

    post_query = "begin;" \
                "drop table " + target_table + ";" \
                "create table if not exists " + target_table + "(like " + stage_table + ");" \
                "delete from " + target_table +";" \
                "insert into " + target_table + " select * from " + stage_table + ";" \
                "drop table " + stage_table + ";" \
                "end;"

    glue_context.write_dynamic_frame_from_jdbc_conf(
        frame=updated_dyf,
        catalog_connection="ph-" + environment + "-redshift-glue",
        connection_options={
            "dbtable": stage_table,
            "database": "ph",
            "extracopyoptions": "TRUNCATECOLUMNS",
            "postactions": post_query
        },
        redshift_tmp_dir=temp_dir
    )

if __name__ == "__main__":
    main()
