import base64
import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from collections import defaultdict

# Set up logging
logging.getLogger().setLevel(logging.ERROR)

sqs_url = os.environ['adherence_sqs_url']

actions = ["created", "updated", "deleted"]
adherence_event_groups = ["sensor_event", "medication", "datalake_message"]
adherence_event_types = [f"{t}_{a}" for t in adherence_event_groups for a in actions]


def lambda_handler(event, context):
    firehose = boto3.client('firehose')
    sqs = boto3.client('sqs')

    decoded_record_data = [json.loads(base64.b64decode(record['body'])) for record in event['Records']]

    full_event_data = event['Records']
    for r in full_event_data:
        r['pubSubEvent'] = json.loads(base64.b64decode(r['body']))['pubSubEvent']
        r['body'] = json.loads(base64.b64decode(r['body']))

    now = datetime.utcnow()
    latencies = []
    types = {r['pubSubEvent']: [] for r in decoded_record_data}
    for r in decoded_record_data:
        if 'replay' not in r['pubSubArgs']:  # if data comes from nifi, "replay" will be at the end of pubsubargs list
            types[r['pubSubEvent']].append({'Data': json.dumps(r)})
        call_time = datetime.strptime(r['callTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
        latency = now - call_time
        latencies.append({'Data': json.dumps({
            'event_type': r['pubSubEvent'],
            'created_datetime': r['callTime'],
            'received_datetime': now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'latency': latency.seconds + (latency.microseconds / 1000000)})
        })

    adherence_messages = defaultdict(list)
    types_raw_data = {r['pubSubEvent']: [] for r in full_event_data}
    for r in full_event_data:
        if 'replay' in r['body']['pubSubArgs']:
            r['body']['pubSubArgs'].remove('replay')
        elif r['pubSubEvent'] in adherence_event_types:
            adherence_messages[r['pubSubEvent']].append({
                'Id': r['body']['pubSubEventId'],
                'MessageBody': json.dumps(r['body'])
            })
        types_raw_data[r['pubSubEvent']].append({'Data': json.dumps(r)})

    logging.info(types)
    for t, data in types.items():
        try:
            if len(data) > 0:
                firehose.put_record_batch(
                    DeliveryStreamName=f'ph-{os.environ["ph_environment"]}-{t}',
                    Records=data
                )
            firehose.put_record_batch(
                DeliveryStreamName=f'ph-{os.environ["ph_environment"]}-raw',
                Records=types_raw_data[t]
            )
            if t in adherence_messages:
                sqs.send_message_batch(QueueUrl=sqs_url, Entries=adherence_messages[t])
        except ClientError as e:
            logging.error(e)
            raise
    if len(types) > 0:
        try:
            firehose.put_record_batch(
                DeliveryStreamName=f'ph-{os.environ["ph_environment"]}-latency',
                Records=latencies
            )
        except ClientError as e:
            logging.error(e)
            raise

    return 'Successfully processed {} records.'.format(len(event['Records']))

