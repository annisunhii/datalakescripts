from ps_transform import EventTransform

print('Loading function')


def lambda_handler(event, context):
    print(event)
    et = EventTransform(event)
    et.run()
    return {'records': et.outputs}
