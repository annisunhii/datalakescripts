from __future__ import print_function
import sys
import json
import math
import calendar
import boto3
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark import StorageLevel
from awsglue.job import Job
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame, ResolveOption
import pyspark.sql.functions as psf
from glue_common import remove_rogue_partitions, try_parsing_date

print('starting to process patient hours',file=sys.stdout)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME','ph_environment','log_level', 'num_partitions',
    'output_dir', 'date_year', 'date_month', 'region'
])
environment = args['ph_environment']
log_level = args['log_level']
region = args['region']
database = "ph_"+environment+"_db_data_lake"
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)
max_stage_errors = 100
max_total_errors = 250
min_date_seconds = 1262304000
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
glue_client = boto3.client('glue', region_name=region)
num_partitions = int(args['num_partitions'])

# compute the ranges of time to curate
range_date = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

if int(args['date_year']) > 0 and int(args['date_month']) > 0:
    date_year = int(args['date_year'])
    date_month = int(args['date_month'])
    range_date = range_date.replace(year=date_year, month=date_month)

range_beginning = range_date
days_in_month = calendar.monthrange(range_date.year, range_date.month)
range_ending = range_date.replace(day=days_in_month[1], hour=23, minute=59, second=59, microsecond=999999)
range_beginning_previous_month = range_beginning - timedelta(days=1)
range_beginning_next_month = range_ending + timedelta(days=1)

print("range_beginning:"+str(range_beginning), file=sys.stdout)
print("range_ending:"+str(range_ending), file=sys.stdout)
print("range_beginning_previous_month:"+str(range_beginning_previous_month), file=sys.stdout)
print("range_beginning_next_month:"+str(range_beginning_next_month), file=sys.stdout)

# build url objects of our 2 s3 paths
output_dir = args['output_dir']+'/'

# read all the weather coordinates and broadcast them
# this is outside of main because of spark scope in custom functions
weather_coordinates_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database = database, table_name ="aqi_coordinates")

print('reading weather coordinates',file=sys.stdout)
weather_coordinates = weather_coordinates_dynamic_frame._rdd.collect()
broadcast_weather_coordinates = sc.broadcast(weather_coordinates)
print('broadcasted weather coordinates',file=sys.stdout)

patient_hours_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("precision", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("date_year", IntegerType(), True),
    StructField("date_month", IntegerType(), True),
    StructField("date_day", IntegerType(), True),
    StructField("date_hour", IntegerType(), True),
    StructField("address", StructType([
        StructField("state_code", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("formatted", StringType(), True),
        StructField("state_or_region", StringType(), True),
        StructField("street", StringType(), True),
        StructField("street2", StringType(), True)
    ]), True),
    StructField("place", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("element", StringType(), True),
        StructField("id", LongType(), True)
    ]), True),
    StructField("weather", StructType([
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("dew_point", IntegerType(), True),
        StructField("wind_speed", IntegerType(), True),
        StructField("o3", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("heat_index", IntegerType(), True),
        StructField("wind_chill", IntegerType(), True),
        StructField("co", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("so2", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("visibility", IntegerType(), True)
    ]), True),
    StructField("weather_coordinate", StructType([
        StructField("id", StringType(), True),
        StructField("version", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("distance", DoubleType(), True)
    ])),
    StructField("location_strategy", StringType(), True)
])

def datetime_to_iso_str(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def sort_by_date(geo_event):
    return try_parsing_date(geo_event['record']['date'])

def unix_time_seconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())

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

def filter_patients_wo_geo_location_first(patient):
    has_geo_location_first = patient['record']['has_geo_location_first']==True \
        and 'geo_location_first' in patient['record'] \
        and patient['record']['geo_location_first'] is not None \
        and 'latitude' in patient['record']['geo_location_first'] \
        and patient['record']['geo_location_first']['latitude'] is not None \
        and 'longitude' in patient['record']['geo_location_first'] \
        and patient['record']['geo_location_first']['longitude'] is not None
        
    return has_geo_location_first

def map_patient_hours(patient):
    # patient
    now = datetime.utcnow()

    # put a min cap on the craziness
    min_date = datetime.utcfromtimestamp(min_date_seconds)

    start_date = try_parsing_date(patient['record']['date_created']).replace(microsecond=0,second=0,minute=0)

    if start_date < min_date:
        start_date = min_date

    if start_date < range_beginning:
        start_date = range_beginning

    start_date = start_date.replace(microsecond=0,second=0,minute=0)

    end_date = now.replace(microsecond=0,second=0,minute=0)
    has_date_deleted = 'date_deleted' in patient['record'] \
        and not patient['record']['date_deleted'] is None
    if has_date_deleted:
        end_date = try_parsing_date(patient['record']['date_deleted']).replace(microsecond=0,second=0,minute=0)

    if end_date > range_ending:
        end_date = range_ending.replace(microsecond=0,second=0,minute=0)

    end_date = end_date.replace(microsecond=0,second=0,minute=0)
    
    hour_delta = timedelta(hours=1)
    day_delta = timedelta(days=2)
    hours = []
    while start_date <= end_date:
        hour_unix_time_seconds = unix_time_seconds(start_date)
        day_0 = start_date.replace(hour=0,minute=0,second=0,microsecond=0)
        day_2 = day_0 - day_delta
        interval_key = str(unix_time_seconds(day_2))+'_'+str(unix_time_seconds(day_0))
        hours.append(((patient['record']['user_id'],interval_key),(patient['record']['group_id'], hour_unix_time_seconds, patient['record']['geo_location_first'])))
        start_date += hour_delta

    return hours

def filter_by_range_date_month(geo):
    d = try_parsing_date(geo['record']['date'])
    return d.year == range_date.year and d.month == range_date.month
    
def map_patient_geo_events(geo):
    d = try_parsing_date(geo['record']['date'])

    day_delta = timedelta(days=1)
    day_0 = d.replace(hour=0,minute=0,second=0,microsecond=0)
    day_minus_1 = day_0 - day_delta
    day_minus_2 = day_minus_1 - day_delta
    day_plus_1 = day_0 + day_delta
    day_plus_2 = day_plus_1 + day_delta

    # we need to create all intervals that this geo could be used
    #[-2, 0] ,[-1, +1], [0, +2]
    result = []
    minus_2_day_0_key = str(unix_time_seconds(day_minus_2))+'_'+str(unix_time_seconds(day_0))
    minus_1_plus_1_key = str(unix_time_seconds(day_minus_1))+'_'+str(unix_time_seconds(day_plus_1))
    day_0_plus_2_key = str(unix_time_seconds(day_0))+'_'+str(unix_time_seconds(day_plus_2))

    result.append(((geo['record']['user_id'], minus_2_day_0_key), geo))
    result.append(((geo['record']['user_id'], minus_1_plus_1_key), geo))
    result.append(((geo['record']['user_id'], day_0_plus_2_key), geo))

    return result

def map_patient_hours_geo(pair):
    # ((uid, interval), ((group_id, hour, geo_location_first), last_geo))

    # only set the last_geo if its timestamp is < the end of the hour.
    last_geo = None
    if pair[1][1] is not None:
        hour = datetime.utcfromtimestamp(pair[1][0][1]).replace(minute=59,second=59,microsecond=999999)
        hour_minus_2_days = hour - timedelta(days=2)
        hour_minus_2_days = hour_minus_2_days.replace(minute=0,second=0,microsecond=0)
        d = try_parsing_date(pair[1][1]['record']['date'])
        if d < hour and d > hour_minus_2_days:
            last_geo = pair[1][1]

    # map to ((uid,hour,group_id), (last_geo, geo_location_first))
    return ((pair[0][0], pair[1][0][1], pair[1][0][0]), (last_geo, pair[1][0][2]))

def reduce_geo_events_to_uid_hour(a, b):
    # (last_geo, geo_location_first)_a, (last_geo, geo_location_first)_b
    result = None
    if a[0] is None and b[0] is None:
        result = a
    elif a[0] is None:
        result = b
    elif b[0] is None:
        result = a
    else:
        a_date = try_parsing_date(a[0]['record']['date'])
        b_date = try_parsing_date(b[0]['record']['date'])
        if a_date > b_date:
            result = a
        else:
            result = b
    return result

def map_patient_hour_geo(pair):
    # ((uid, hour, group_id), (last_geo, geo_location_first))
    dt = datetime.utcfromtimestamp(pair[0][1]).replace(minute=0,second=0,microsecond=0)

    # okay, we want to use a geo point <= 72 hours if we gots em
    # otherwise use geo_location_first as the lat/lng then
    # annotate how we derived the weather_coordinate
    latitude = None
    longitude = None
    precision = None
    address = None
    place = None
    strategy = None
    weather_coordinate = None
    if not pair[1][0] is None:
        latitude = pair[1][0]['record']['latitude']
        longitude = pair[1][0]['record']['longitude']
        weather_coordinate = pair[1][0]['record']['weather_coordinate']
        strategy = 'last_48_hours'

        # check for precision
        has_precision = 'precision' in pair[1][0]['record'] \
            and pair[1][0]['record']['precision'] is not None
        if has_precision:
            precision = pair[1][0]['record']['precision']
        
        # check for address
        has_address = 'address' in pair[1][0]['record'] \
            and pair[1][0]['record']['address'] is not None
        if has_address:
            address = pair[1][0]['record']['address']

        # check for place
        has_place = 'place' in pair[1][0]['record'] \
            and pair[1][0]['record']['place'] is not None
        if has_place:
            place = pair[1][0]['record']['place']
        
    else: 
        # fall back to the first geo location for the patient
        latitude = pair[1][1]['latitude']
        longitude = pair[1][1]['longitude']
        weather_coordinate = pair[1][1]['weather_coordinate']
        strategy = 'geo_location_first'

        # check for precision
        has_precision = 'precision' in pair[1][1] \
            and pair[1][1]['precision'] is not None
        if has_precision:
            precision = pair[1][1]['precision']

        # check for address
        has_address = 'address' in pair[1][1] \
            and pair[1][1]['address']
        if has_address:
            address = pair[1][1]['address']

        # check for place
        has_place = 'place' in pair[1][1] \
            and pair[1][1]['place'] is not None
        if has_place:
            place = pair[1][1]['place']

    # build the resulting hour w/weather_coordinate set!?!?
    result = {
        'user_id': pair[0][0],
        'latitude': latitude,
        'longitude': longitude,
        'precision': precision,
        'date': datetime_to_iso_str(dt),
        'group_id': pair[0][2],
        'date_year': dt.year,
        'date_month': dt.month,
        'date_day': dt.day,
        'date_hour': dt.hour,
        'address': address,
        'place': place,
        'weather_coordinate': weather_coordinate,
        'location_strategy': strategy
    }
    return result

def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [psf.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

def main():
    # read the patients
    print('reading patients',file=sys.stdout)
    patients_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
       database = database,
       table_name = "patients",
       transformation_ctx="create_dynamic_frame_from_catalog_patients",
       additional_options={
           'minPartitions': num_partitions
       }
    )
    if log_level == 'debug':
       print('patients_dynamic_frame',file=sys.stdout)
       patients_dynamic_frame.printSchema()
       patients_dynamic_frame.show(5)
    print('read patients',file=sys.stdout)

    print('reading geo locations',file=sys.stdout)
    patient_geo_location_created_events = glue_context.create_dynamic_frame.from_catalog(
        database = database,
        table_name = "patient_geo_location_created_by_date",
        transformation_ctx="create_dynamic_frame_from_catalog_patient_geo_location_created",
        additional_options={
            'minPartitions': num_partitions
        },
        push_down_predicate="date_year=="+str(range_beginning.year)+" AND date_month=="+str(range_beginning.month)
        #group_id='58dea410ffaff226079b2373' AND 
    )
        
    if log_level == 'debug':
        print('patient_geo_location_created_events',file=sys.stdout)
        patient_geo_location_created_events.printSchema()
        patient_geo_location_created_events.show(5)
    print('read geo locations',file=sys.stdout)

    # let's determine the interval of all patient hours
    #.filter(lambda p: p['record']['group_id']=="58dea410ffaff226079b2373") \
    patient_hours = patients_dynamic_frame._rdd \
        .filter(filter_patients_wo_geo_location_first) \
        .flatMap(map_patient_hours)

    # join the hours & geo events by uid
    print('building patient_geo_events',file=sys.stdout)
    patient_geo_events = patient_geo_location_created_events._rdd \
        .flatMap(map_patient_geo_events)
    print('built patient_geo_events',file=sys.stdout)

    print('building patient_hours_geo_joined',file=sys.stdout)
    # okay so this is weird, there's no way in RDD to join with operators besides ==
    # otherwise we could do join uid=uid & geo.hour <= patient.hour & geo.hour >= patient.hour -72
    # we can do joins w/operators in dataframes but we need a schema so...
    # the hack in RDD is that patient_hours is really the interval uid+[day-2, day_0]
    # and geo is uid+[day-2, 0], uid+[day-1, day+1], uid+[day_0, day+2] so we'll 3x each geo point
    # then reduce on hour to the last geo point in the interval
    # long term, we should just break to dataframes and use operators in the join & do 1x join

    patient_hours_geo_joined = patient_hours \
        .leftOuterJoin(patient_geo_events, numPartitions=num_partitions)
    print('built patient_hours_geo_joined',file=sys.stdout)

    # now time to assign locations per hour
    print('building patient_hours_with_location',file=sys.stdout)   
    patient_hours_with_location = patient_hours_geo_joined \
        .map(map_patient_hours_geo) \
        .reduceByKey(reduce_geo_events_to_uid_hour, numPartitions=num_partitions) \
        .map(map_patient_hour_geo)
    print('built patient_hours_with_location',file=sys.stdout)

    # load the aq table & key on hour+location_id
    print('reading air quality data',file=sys.stdout)
    aq_hours_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database = database,
        table_name = "aq",
        transformation_ctx="create_dynamic_frame_from_catalog_aq",
        additional_options={
            'minPartitions': num_partitions
        },
        push_down_predicate="date_year=="+str(range_beginning.year)+" AND date_month=="+str(range_beginning.month)
    )
    if log_level == 'debug':
        print('aq_hours_dynamic_frame',file=sys.stdout)
        aq_hours_dynamic_frame.printSchema()
        aq_hours_dynamic_frame.show(5)
    
    aq_hours = aq_hours_dynamic_frame.toDF()
    aq_hours = aq_hours.withColumnRenamed('guid', 'location_id')

    if not 'datetime' in aq_hours.columns:
        aq_hours = aq_hours.withColumn("datetime", psf.lit(None).cast('string'))

    aq_hours = aq_hours \
        .withColumn("hour", psf.date_trunc("hour", aq_hours.datetime.cast('timestamp'))) \
        .dropDuplicates(subset=['location_id','hour'])
    print('read air quality data',file=sys.stdout)

    # load the weather table & key on hour+location_id
    print('reading weather data',file=sys.stdout)
    weather_hours_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database = database,
        table_name = "weather",
        transformation_ctx="create_dynamic_frame_from_catalog_weather",
        additional_options={
            'minPartitions': num_partitions
        },
        push_down_predicate="date_year=="+str(range_beginning.year)+" AND date_month=="+str(range_beginning.month)
    )
    if log_level == 'debug':
        print('weather_hours_dynamic_frame',file=sys.stdout)
        weather_hours_dynamic_frame.printSchema()
        weather_hours_dynamic_frame.show(5)
    
    weather_hours = weather_hours_dynamic_frame.toDF()
    weather_hours = weather_hours.withColumnRenamed('guid', 'location_id')

    if not 'datetime' in weather_hours.columns:
        weather_hours = weather_hours.withColumn("datetime", psf.lit(None).cast('string'))
    weather_hours = weather_hours \
        .withColumn(
            "hour", 
            psf.date_trunc("hour", psf.when(
                weather_hours.datetime.isNotNull(),
                weather_hours.datetime.cast("timestamp")
            ).otherwise(
                weather_hours.date_observation.cast("timestamp")
            ))
        ) \
        .dropDuplicates(subset=['location_id','hour'])
    print('read weatherdata',file=sys.stdout)

    environmental_hours = weather_hours \
        .join(aq_hours, ['location_id', 'hour'], 'full_outer') \

    environmental_hours = environmental_hours \
        .select(psf.col('location_id'), 
                psf.col('hour'),
                psf.col('pm25_concentration').cast('double').alias('weather_pm25'),
                psf.col('pm10_concentration').cast('double').alias('weather_pm10'),
                psf.col('dew_point').cast('int').alias('weather_dew_point'),
                psf.col('wind_speed').cast('int').alias('weather_wind_speed'),
                psf.col('o3_concentration').cast('double').alias('weather_o3'),
                psf.col('no2_concentration').cast('double').alias('weather_no2'),
                psf.col('heat_index').cast('int').alias('weather_heat_index'),
                psf.col('wind_chill').cast('int').alias('weather_wind_chill'),
                psf.col('co_concentration').cast('double').alias('weather_co'),
                psf.col('wind_direction').cast('int').alias('weather_wind_direction'),
                psf.col('so2_concentration').cast('double').alias('weather_so2'),
                psf.col('breezometer_aqi').cast('int').alias('weather_aqi'),
                psf.col('pressure').cast('double').alias('weather_pressure'),
                psf.col('relative_humidity').cast('int').alias('weather_humidity'),
                psf.col('temperature').cast('int').alias('weather_temperature'),
                psf.col('visibility').cast('int').alias('weather_visibility')) \
        .withColumn("env_date_year", psf.year(environmental_hours.hour)) \
        .withColumn("env_date_month", psf.month(environmental_hours.hour)) \
        .withColumn("env_date_day", psf.dayofmonth(environmental_hours.hour)) \
        .withColumn("env_date_hour", psf.hour(environmental_hours.hour)) \
        .drop("hour")

    patient_hours_with_location_df = glue_context.create_dynamic_frame_from_rdd(
        patient_hours_with_location,
        "patient_hours_With_location_df",
        schema=patient_hours_schema
    ).toDF()

    df = patient_hours_with_location_df \
        .drop('weather') \
        .join(environmental_hours, [
            patient_hours_with_location_df.weather_coordinate.id == environmental_hours.location_id,
            patient_hours_with_location_df.date_year == environmental_hours.env_date_year,
            patient_hours_with_location_df.date_month == environmental_hours.env_date_month,
            patient_hours_with_location_df.date_day == environmental_hours.env_date_day,
            patient_hours_with_location_df.date_hour == environmental_hours.env_date_hour
        ], 'left_outer')
    
    df = df.drop(
        'location_id',
        'env_date_year',
        'env_date_month',
        'env_date_day',
        'env_date_hour'
    )
    flat_df = flatten_df(df)


    # write our new month results to temp
    print('starting write of partitioned temp output dir', file=sys.stdout)
    flat_df.repartition("group_id", "date_year", "date_month") \
        .write.mode("overwrite").format("parquet") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy('group_id', 'date_year', 'date_month') \
        .save(output_dir)
    print('finished write of partitioned temp output_dir', file=sys.stdout)

    print('wrote the patient_hours',file=sys.stdout)

    # TL-720 delete any rogue partitions
    remove_rogue_partitions(
        glue_client,
        database,
        "patient_hours",
        flat_df,
        date_year=range_beginning.year,
        date_month=range_beginning.month
    )

    job.commit()

if __name__ == "__main__":
    main()
