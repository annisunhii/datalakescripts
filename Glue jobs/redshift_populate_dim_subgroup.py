#from __future__ import print_function
import sys
from datetime import datetime, timedelta, timezone
from awsglue.transforms import ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StringType, StructField, \
    BooleanType, TimestampType, IntegerType
from glue_common import DEFAULT_NULL_DATETIME, DEFAULT_NULL_INTEGER, \
    DEFAULT_NULL_DOUBLE, DEFAULT_NULL_BOOLEAN, DEFAULT_NULL_STRING, try_parsing_date, \
    dyf_for_bronze_table, rdd_for_bronze_table, rdd_for_table



args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'ph_environment', 'log_level', 'region', 'num_partitions', 's3_bucket'])
environment = args['ph_environment']
log_level = args['log_level']
region = args['region']
region_abbrev = region[:2]
database = "ph_"+environment+"_db_data_lake"
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)
temp_dir = args['TempDir']
s3_bucket = args['s3_bucket']
current_timestamp = datetime.utcnow()
emptyRDD = spark.sparkContext.emptyRDD()
num_partitions = int(args['num_partitions'])

MAX_STAGE_ERRORS = 100
MAX_TOTAL_ERRORS = 250



dim_subgroup_schema = StructType([
    StructField("subgroup_id",
                StringType(),       False),
    StructField("subgroup_name",
                StringType(),       True),
    StructField("subgroup_display_name",
                StringType(),       True),
    StructField("subgroup_type",
                StringType(),       True),
    StructField("deleted",
                BooleanType(),      False),
    StructField("ph_group_id",
                StringType(),       False),
    StructField("created_datetime",
                TimestampType(),    False),
    StructField("deleted_datetime",
                TimestampType(),    True),
    StructField("datetime_modified", 
                TimestampType(),    True)
                
])



def main():
    '''
    load subgroup_added, subgroup_updated, subgroup_deleted bronze tables 
    '''
    #created
    subgroup_created = rdd_for_bronze_table(
        table="subgroup_created",
        glue_context=glue_context,
        s3_bucket=s3_bucket,
        job_name="subgroup_created",
        sc= sc
    )   
        

    # updated
    subgroup_updated = rdd_for_bronze_table(
        table="subgroup_updated",
        glue_context=glue_context,
        s3_bucket=s3_bucket,
        job_name="subgroup_updated",
        sc= sc
    )  

    # deleted
    subgroup_deleted = rdd_for_bronze_table(
        table="subgroup_deleted",
        glue_context=glue_context,
        s3_bucket=s3_bucket,
        job_name="subgroup_deleted",
        sc= sc
    ) 
    
    

    # apply transformation  
    #updated & created
    dim_subgroup= subgroup_created\
        .union(subgroup_updated)\
        .union(subgroup_deleted)\
        .keyBy(lambda x: (x['record']['subgroup_id'])) \
        .reduceByKey(reduce_by_greatest_call_time, numPartitions=num_partitions) \
        .map(map_subgroup_with_datetime_modified_column)


 
    dim_subgroup = dim_subgroup \
        .leftOuterJoin(subgroup_deleted.keyBy(
        lambda x: (x['record']['subgroup_id'])), numPartitions=num_partitions) \
        .map(map_subgroup_with_deleted_columns) #add 'deleted_datetime' and 'deleted' for deleted rows
        
    dim_subgroup = dim_subgroup \
        .leftOuterJoin(subgroup_created.keyBy(
        lambda x: (x['record']['subgroup_id'])), numPartitions=num_partitions) \
        .map(map_subgroup_with_date_created_column)#add 'created_datetime'


    dim_subgroup= dim_subgroup.map(lambda x: x[1])
    dim_subgroup= dim_subgroup.map(map_dim_subgroup)
    
    

    debug_statement(
        dim_subgroup,
        'rdd',
        'dim_subgroup: Started Redshift write',
        log_level=log_level
    )
    
    
    dim_subgroup_df= spark.createDataFrame(dim_subgroup,schema=dim_subgroup_schema)
    dim_subgroup_df=dim_subgroup_df.withColumn("region", F.lit(region_abbrev))
    
    
    dim_subgroup_df = DynamicFrame.fromDF(
        dataframe=dim_subgroup_df,
        glue_ctx=glue_context,
        name='dim_subgroup_df'
       )
    
    dim_subgroup_df = ResolveChoice.apply(
        frame=dim_subgroup_df,
        stageThreshold=MAX_STAGE_ERRORS,
        totalThreshold=MAX_TOTAL_ERRORS,
        specs=get_resolve_choice_specs_from_schema(dim_subgroup_schema)
    )
    
    
    dim_subgroup_df = DropNullFields.apply(
        frame=dim_subgroup_df,
        stageThreshold=MAX_STAGE_ERRORS,
        totalThreshold=MAX_TOTAL_ERRORS
    )
    
    upsert_table_into_redshift(
        dim_subgroup_df,
        target_table_name='dim_subgroup',
        upsert_key_columns=['subgroup_id'],
        glue_context=glue_context,
        environment=environment,
        region=region,
        temp_dir=temp_dir
    )

    debug_statement(
        dim_subgroup,
        'df',
        'dim_subgroup written to Redshift'
    )
 



 
   
def get_resolve_choice_specs_from_schema(schema):
    type_map = {
        'BooleanType':      'cast:boolean',
        'DateType':         'cast:date',
        'DoubleType':       'cast:float',
        'IntegerType':      'cast:int',
        'LongType':         'cast:int',
        'StringType':       'cast:string',
        'TimestampType':    'cast:timestamp'
    }

    return [(col.name, type_map[str(col.dataType)]) for col in schema]


def debug_statement(data, data_type, print_message, log_level=None, uid=None):
    print(print_message)
    if log_level == 'debug':
        print("Row count:", data.count())
        if data_type == 'rdd':
            data.take(5)
        elif data_type in ('df', 'dyf'):
            if uid is not None:
                data = data.filter(data.uid == uid)
            data.show(5)
            data.printSchema()


def upsert_table_into_redshift(dyf, target_table_name,
                               upsert_key_columns: list,
                               glue_context, environment, region, temp_dir,
                               target_redshift_schema='analytics'):
    stage_table = f'{target_redshift_schema}.stage_{target_table_name}'
    target_table = f'{target_redshift_schema}.{target_table_name}'

    upsert_where = ' and '.join(
        [f'{stage_table}.{col} = {target_table}.{col}' for col in upsert_key_columns])

    pre_query = f'''
        drop table if exists {stage_table};
        create table {stage_table} as select * from {target_table} where 1=2;'''

    post_query = f'''
        begin;
        delete from {target_table} using {stage_table} where {upsert_where};
        insert into {target_table} select * from {stage_table};
        drop table {stage_table}; 
        end;'''

    glue_context.write_dynamic_frame_from_jdbc_conf(
        frame=dyf,
        catalog_connection="ph-" + environment + "-redshift-glue",
        connection_options={
            "preactions": pre_query,
            "dbtable": stage_table,
            "database": 'ph',
            "postactions": post_query,
            "extracopyoptions": f"REGION '{region}'"
        },
        redshift_tmp_dir=temp_dir+target_table_name+'/')


def map_subgroup_with_deleted_columns(pair):
    # (subgroup_id, (last_subgroup_event, subgroup_deleted))
    deleted = pair[1][1] is not None
    if deleted:
        pair[1][0]['record']['deleted_datetime'] = try_parsing_date(pair[1][1]['record']['call_time'])

    else:
        pair[1][0]['record']['deleted_datetime'] = try_parsing_date(None)
    pair[1][0]['record']['deleted'] = deleted

    return (pair[0], pair[1][0]['record'])
    

def map_subgroup_with_date_created_column(pair):
    # (subgroup_id, (last_subgroup_event, subgroup_created))
    created = pair[1][1] is not None
    if created:
        pair[1][0]['created_datetime'] = try_parsing_date(pair[1][1]['record']['call_time'])

    return (pair[0], pair[1][0])

def map_subgroup_with_datetime_modified_column(row):
    row[1]['record']['datetime_modified'] = try_parsing_date(row[1]['record']['call_time'])
    return row

 


def reduce_by_greatest_call_time(a, b):
    # patient_event_a, patient_event_b
    result = None
    a_date = try_parsing_date(a['record']['call_time'])
    b_date = try_parsing_date(b['record']['call_time'])
    if a_date > b_date:
        result = a
    else:
        result = b
    return result


#rename column names
def map_dim_subgroup(row):
    row['ph_group_id'] = row['group_id']
    row['subgroup_display_name'] = row['display_name']
    row['subgroup_name'] = row['name']
    row['subgroup_type'] = row['type']

    return row


def try_parsing_date(text):
    if type(text) is str and not text.isdigit():
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z'):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')
    elif type(text) is str and text.isdigit():
        return datetime.fromtimestamp(int(text) / 1e3)
    elif type(text) is int:
        return datetime.fromtimestamp(text / 1e3)
    elif type(text) is datetime:
        return datetime
    elif text is None:
        return None
    else:
        raise ValueError('invalid date data type: '+str(type(text)))



if __name__ == "__main__":
    main()


