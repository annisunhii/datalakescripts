import base64
import json

class EventTransform(object):
    def __init__(self, event):
        self.invocation_id = event['invocationId']
        self.records = event['records']
        self.outputs = []

    def load_record(self, record): #static but who cares
        return self.map_event(json.loads(base64.b64decode(record['data'])))

    def map_event(self, event):
        result = {}
        result['pubSubEvent'] = event['pubSubEvent']
        result['userId'] = event['body']['userId']
        result['groupId'] = event['body']['groupId']
        result['callTime'] = event['body']['callTime']
        result['pubSubArgs'] = event['body']['pubSubArgs']
        result['pubSubEventId'] = event['body']['pubSubEventId']
        result['version'] = event['body']['version']
        result['body'] = json.dumps(event['body']['body'])
        return result

    def run(self):
        for r in self.records:
            raw_data = self.load_record(r)
            print(raw_data)
            self.outputs.append(self.output(r, raw_data))

    def output(self, record, data): #static but who cares
        partition_keys = {"pubSubEvent": data["pubSubEvent"]}
        del data["pubSubEvent"]
        return {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8'),
            'metadata': {'partitionKeys': partition_keys},
        }