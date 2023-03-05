import base64
import json
import logging
import boto3
import boto3.session
import sys
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from awsglue.utils import getResolvedOptions

config = TransferConfig(max_concurrency=2)
# Set up logging
logging.getLogger().setLevel(logging.ERROR)
args = getResolvedOptions(sys.argv, [
    'ph_environment',
    'bucket_name',
    'ph_prefix'
])
ph_environment = args['ph_environment']
bucket_name = args['bucket_name']
#failed_event= args['failed_event']
sessions3 = boto3.session.Session()
s3 = sessions3.resource('s3')
firehose = boto3.client('firehose')
bucket = s3.Bucket(bucket_name)

#return the list of rawData
def decode_raw_data(json_list):
    output=[]
    for item in json_list:
        output.append(json.loads(base64.b64decode(item["rawData"])))
    return output

def failed_events():
    failed_event_list=[]
    for obj in bucket.objects.filter(Prefix="ingress/bronze_tables/error_firehose/"):
        content_object= s3.Object(bucket.name, obj.key)
        file_name = str(content_object.key)
        #weather_updatedprocessing-failed    ----> weather_updated
        failed_event=  file_name.split("/")[3].replace('processing-failed','')
        failed_event_list.append(failed_event)

    return list(set(failed_event_list))


def process_failed_event(failed_event):

    prefix_event = str(args['ph_prefix']+failed_event+"processing-failed/")
    DeliveryStreamName=str("ph-"+args['ph_environment']+"-"+failed_event+"-"+"retry")

    for obj in bucket.objects.filter(Prefix=prefix_event):

        content_object= s3.Object(bucket.name, obj.key)
        file_name=content_object.key

        if content_object.get()['Body'].read().decode('utf-8'):
            #create a list of strings, split on new lines
            file_content_list = content_object.get()['Body'].read().decode('utf-8').split("\n")

            #convert the strings(value) to actual dictionaries
            file_content_list_json = [json.loads(x) for x in file_content_list if x != ""] 

            #return a list of dictionaries(rawdata)
            decoded_content_list = decode_raw_data(file_content_list_json) 

            print("there are ", len(file_content_list_json), 'failed records in the ', file_name ,'need to be processed')

            types = {r['pubSubEvent']: [] for r in decoded_content_list}  

            for r in decoded_content_list:
                types[r['pubSubEvent']].append({'Data': json.dumps(r)})

            batch_size=100    
            for key, data in types.items():
                for i in range(0, len(data), batch_size):
                    try:
                        result=firehose.put_record_batch(
                            DeliveryStreamName=DeliveryStreamName,
                            Records=data[i:i+batch_size]
                            )

                    except ClientError as e:
                        logging.error(e)
                        raise

            print('Successfully re-processed {} records '.format(len(data)), 'in file: ', obj.key)

        #delete the processed error file for every object in this event folder
        s3.Object(bucket.name, obj.key).delete()



def main():
    failed_event_list=failed_events()
    for failed_event in failed_event_list:
        print('start re-process failed event:', failed_event)
        process_failed_event(failed_event)
        print('Successfully delete and re-processed failed event:', failed_event)

        
if __name__ == "__main__":
    main()