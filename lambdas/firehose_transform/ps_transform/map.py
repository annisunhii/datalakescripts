import ast
import base64
import csv
import json
import sys
from datetime import datetime, timedelta

DEFAULT_NULL_DATETIME = "1970-01-01T00:00:00.000Z"
DEFAULT_NULL_INTEGER = -1
DEFAULT_NULL_DOUBLE = -1.0
DEFAULT_NULL_BOOLEAN = False
DEFAULT_NULL_STRING = "-1"

def dt_with_offset_parse(t):
    # python 2.7 sucks w/+|-HH:MM offsets in ISO8601 dates
    # therefore, the best native 2.7 solution
    # https://stackoverflow.com/questions/1101508/how-to-parse-dates-with-0400-timezone-string-in-python/23122493#23122493
    ret = datetime.strptime(t[0:16], '%Y-%m-%dT%H:%M')
    if t[18] == '+':
        ret -= timedelta(hours=int(t[19:22]), minutes=int(t[23:]))
    elif t[18] == '-':
        ret += timedelta(hours=int(t[19:22]), minutes=int(t[23:]))
    return ret


def datetime_to_iso_str(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def try_parsing_date(text):
    if type(text) is str and not text.isdigit():
        if text.endswith("+00:00"):
            text = text[:19]
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%MZ'):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
        raise ValueError(f'no valid date format found for: {text}')
    elif type(text) is str and text.isdigit():
        return datetime.utcfromtimestamp(int(text) / 1e3)
    elif type(text) is int:
        return datetime.utcfromtimestamp(text / 1e3)
    elif type(text) is datetime:
        return datetime
    elif text is None:
        return None
    else:
        raise ValueError('invalid date data type: '+str(type(text)))

def get_pubSubArgs(raw):
    pubSubArgs = [i if i==None else str(i) for i in raw['pubSubArgs']]
    return pubSubArgs

class EventTransform(object):
    def __init__(self, event):
        self.invocation_id = event['invocationId']
        self.records = event['records']
        self.event_type = event['deliveryStreamArn'].split('/')[-1].split('-')[-1]
        self.outputs = []
        self.pub_sub_events = {
            "address_updated": {
                "transform_fx": self.map_event,
            },
            "adherence_updated": {
                "transform_fx": self.map_adherence_updated,
            },
            "admin_created": {
                "transform_fx": self.map_provider,
            },
            "admin_deleted": {
                "transform_fx": self.map_provider,
            },
            "alert_exacerbation_created": {
                "transform_fx": self.map_alert,
            },
            "alert_at_risk_created": {
                "transform_fx": self.map_alert,
            },
            "alert_adherence_created": {
                "transform_fx": self.map_alert,
            },
            "alert_controller_volume_created": {
                "transform_fx": self.map_alert,
            },
            "alert_deleted": {
                "transform_fx": self.map_alert,
            },
            "alert_missed_dose_created": {
                "transform_fx": self.map_alert,
            },
            "alert_quiet_sensor_created": {
                "transform_fx": self.map_alert,
            },
            "alert_rescue_usage_created": {
                "transform_fx": self.map_alert,
            },
            "alert_rescue_volume_created": {
                "transform_fx": self.map_alert,
            },
            "alert_transition_created": {
                "transform_fx": self.map_alert,
            },
            "app_heartbeat_created": {
                "transform_fx": self.map_app_heartbeat_created,
            },
            "caregiver_created": {
                "transform_fx": self.map_caregiver,
            },
            "caregiver_deleted": {
                "transform_fx": self.map_caregiver,
            },
            "datalake_message_created": {
                "transform_fx": self.map_datalake_message_created,
            },
            "external_id_created": {
                "transform_fx": self.map_external_id,
            },
            "external_id_deleted": {
                "transform_fx": self.map_external_id,
            },
            "medication_created": {
                "transform_fx": self.map_medication,
            },
            "medication_deleted": {
                "transform_fx": self.map_medication,
            },
            "medication_updated": {
                "transform_fx": self.map_patient,
            },
            "note_updated": {
                "transform_fx": self.map_event,
            },
            "patient_created": {
                "transform_fx": self.map_patient,
            },
            "patient_deleted": {
                "transform_fx": self.map_patient,
            },
            "patient_followed": {
                "transform_fx": self.map_patient,
            },
            "patient_geo_location_created": {
                "transform_fx": self.map_patient_geo_location_created,
            },
            "patient_login": {
                "transform_fx": self.map_patient_login,
            },
            "patient_reactivated": {
                "transform_fx": self.map_patient,
            },
            "patient_score_updated": {
                "transform_fx": self.map_patient_score_updated,
            },
            "patient_subgroup_added": {
                "transform_fx": self.map_patient_subgroup,
            },
            "patient_subgroup_removed": {
                "transform_fx": self.map_patient_subgroup,
            },
            "patient_unfollowed": {
                "transform_fx": self.map_patient,
            },
            "patient_updated": {
                "transform_fx": self.map_patient,
            },
            "physician_created": {
                "transform_fx": self.map_provider,
            },
            "physician_deleted": {
                "transform_fx": self.map_provider,
            },
            "preempt_updated": {
                "transform_fx": self.map_event,
            },
            "prospect_created": {
                "transform_fx": self.map_prospect,
            },
            "prospect_deleted": {
                "transform_fx": self.map_prospect,
            },
            "prospect_updated": {
                "transform_fx": self.map_prospect,
            },
            "sensor_created": {
                "transform_fx": self.map_patient,
            },
            "sensor_deleted": {
                "transform_fx": self.map_patient,
            },
            "sensor_event_created": {
                "transform_fx": self.map_event,
            },
            "sensor_event_updated": {
                "transform_fx": self.map_event,
            },
            "sensor_event_deleted": {
                "transform_fx": self.map_event,
            },
            "sensor_first_sync": {
                "transform_fx": self.map_sensor_sync,
            },
            "sensor_message_created": {
                "transform_fx": self.map_sensor_message_created,
            },
            "sensor_synchronized": {
                "transform_fx": self.map_sensor_sync,
            },
            "subgroup_created": {
                "transform_fx": self.map_subgroup,
            },
            "subgroup_updated": {
                "transform_fx": self.map_subgroup,
            },
            "subgroup_deleted": {
                "transform_fx": self.map_subgroup,
            },
            "symptom_created": {
                "transform_fx": self.map_event,
            },
            "symptom_deleted": {
                "transform_fx": self.map_event,
            },
            "trigger_created": {
                "transform_fx": self.map_event,
            },
            "trigger_deleted": {
                "transform_fx": self.map_event,
            },
            "weather_updated": {
                "transform_fx": self.map_event,
            },
            "shipment_status_updated": {
                "transform_fx": self.map_shipment_status_updated,
            },
            "asthma_forecast_created": {
                "transform_fx": self.map_asthma_forecast_created,
            },
            "group_created": {
                "transform_fx": self.map_group,
            },
            "group_updated": {
                "transform_fx": self.map_group,
            },
            "act_created": {
                "transform_fx": self.map_act_created,
            },
            "cat_created": {
                "transform_fx": self.map_cat_created,
            },
            "questionnaire_response_created": {
                "transform_fx": self.map_questionnaire_response_created,
            },
            "spirometry_session_created": {
                "transform_fx": self.map_spirometry_session_created
            }
        }

    def get_outputs(self):
        return self.outputs

    def load_record(self, record): #static but who cares
        return json.loads(base64.b64decode(record['data']))

    def run(self):
        map_func = self.pub_sub_events[self.event_type]['transform_fx']

        for r in self.records:
            raw_data = self.load_record(r)
            data = map_func(raw_data)
            self.outputs.append(self.output(r, data))

    def output(self, record, data): #static but who cares
        return {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8')
        }

    def map_provider(self, event):
        try:
            result = {}
            result['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            result['user_id'] = event["userId"]
            result['group_id'] = event["groupId"]
            result['call_time'] = datetime_to_iso_str(call_time)
            result['pub_sub_event_id'] = event['pubSubEventId']
            result['given_name'] = event['body']["givenName"]
            result['time_zone'] = event['body']["timeZone"]
            result['family_name'] = event['body']['familyName']
            result['email'] = event['body']['email']
            result['language'] = event['body']['language']
            result['date_created'] = event['body']['createdDate']
            result['role'] = event['body']['role']
            result['date_year'] = call_time.year
            result['date_month'] = call_time.month
            result['preferences'] = {}
            result['group'] = {
                'name': event['body']['group']['name'],
                'display_name': event['body']['group']['displayName']
            }

            has_preferences = 'preferences' in event['body'] \
                              and event['body']['preferences'] is not None
            if has_preferences:
                has_portal_welcome_done = 'portalWelcomeDone' in event['body']['preferences'] \
                                          and event['body']['preferences']['portalWelcomeDone'] is not None
                if has_portal_welcome_done:
                    result['preferences']['portal_welcome_done'] = event['body']['preferences']['portalWelcomeDone']

                has_weekend_digests = 'weekendDigests' in event['body']['preferences'] \
                                      and event['body']['preferences']['weekendDigests'] is not None
                if has_weekend_digests:
                    result['preferences']['weekend_digests'] = event['body']['preferences']['weekendDigests']

                has_patients_hidden = 'patientsHidden' in event['body']['preferences'] \
                                      and event['body']['preferences']['patientsHidden'] is not None
                if has_patients_hidden:
                    result['preferences']['patients_hidden'] = event['body']['preferences']['patientsHidden']

                has_last_announcement = 'lastAnnouncement' in event['body']['preferences'] \
                                        and event['body']['preferences']['lastAnnouncement'] is not None
                if has_last_announcement:
                    result['preferences']['last_announcement'] = event['body']['preferences']['lastAnnouncement']
                else:
                    result['preferences']['last_announcement'] = ""

            result['notifications'] = {
                'digest_email': event['body']['notifications']['digest']['email']
            }

            has_phone = 'phone' in event['body'] \
                        and event['body']['phone'] is not None
            if has_phone:
                result['phone'] = event['body']['phone']
            return result
        except Exception as e:
            print('error in map_provider', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_caregiver(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['given_name'] = event['body']["givenName"]
            event['time_zone'] = event['body']["timeZone"]
            event['family_name'] = event['body']['familyName']
            event['email'] = event['body']['email']
            event['language'] = event['body']['language']
            event['date_created'] = event['body']['createdDate']
            event['role'] = event['body']['role']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month
            event['group'] = {
                'name': event['body']['group']['name'],
                'display_name': event['body']['group']['displayName']
            }

            has_phone = 'phone' in event['body'] \
                        and event['body']['phone'] is not None
            if has_phone:
                event['phone'] = event['body']['phone']

            # notifications
            event['notifications'] = {
                'digest_email': event['body']['notifications']['digest']['email']
            }
            has_environmental = 'environmental' in event['body']['notifications'] \
                                and event['body']['notifications']['environmental'] is not None
            if has_environmental:
                event['notifications']['environmental_email'] = event['body']['notifications']['environmental']['email']
                event['notifications']['environmental_sms'] = event['body']['notifications']['environmental']['sms']
                event['notifications']['environmental_push'] = event['body']['notifications']['environmental']['push']

            has_quiet_sensor = 'quietSensor' in event['body']['notifications'] \
                               and event['body']['notifications']['quietSensor'] is not None
            if has_quiet_sensor:
                event['notifications']['quiet_sensor_email'] = event['body']['notifications']['quietSensor']['email']
                event['notifications']['quiet_sensor_sms'] = event['body']['notifications']['quietSensor']['sms']
                event['notifications']['quiet_sensor_push'] = event['body']['notifications']['quietSensor']['push']

            has_goal = 'goal' in event['body']['notifications'] \
                       and event['body']['notifications']['goal'] is not None
            if has_goal:
                event['notifications']['goal_email'] = event['body']['notifications']['goal']['email']
                event['notifications']['goal_sms'] = event['body']['notifications']['goal']['sms']
                event['notifications']['goal_push'] = event['body']['notifications']['goal']['push']

            has_rescue_usage = 'rescueUsage' in event['body']['notifications'] \
                               and event['body']['notifications']['rescueUsage'] is not None
            if has_rescue_usage:
                event['notifications']['rescue_usage_email'] = event['body']['notifications']['rescueUsage']['email']
                event['notifications']['rescue_usage_sms'] = event['body']['notifications']['rescueUsage']['sms']
                event['notifications']['rescue_usage_push'] = event['body']['notifications']['rescueUsage']['push']

            has_missed_dose = 'missedDose' in event['body']['notifications'] \
                              and event['body']['notifications']['missedDose'] is not None
            if has_missed_dose:
                event['notifications']['missed_dose_email'] = event['body']['notifications']['missedDose']['email']
                event['notifications']['missed_dose_sms'] = event['body']['notifications']['missedDose']['sms']
                event['notifications']['missed_dose_push'] = event['body']['notifications']['missedDose']['push']

            has_act_invite = 'actInvite' in event['body']['notifications'] \
                             and event['body']['notifications']['actInvite'] is not None
            if has_act_invite:
                event['notifications']['act_invite_email'] = event['body']['notifications']['actInvite']['email']
                event['notifications']['act_invite_sms'] = event['body']['notifications']['actInvite']['sms']
                event['notifications']['act_invite_push'] = event['body']['notifications']['actInvite']['push']

            has_notes = 'notes' in event['body']['notifications'] \
                        and event['body']['notifications']['notes'] is not None
            if has_notes:
                event['notifications']['notes_email'] = event['body']['notifications']['notes']['email']
                event['notifications']['notes_sms'] = event['body']['notifications']['notes']['sms']
                event['notifications']['notes_push'] = event['body']['notifications']['notes']['push']

            has_transition = 'transition' in event['body']['notifications'] \
                             and event['body']['notifications']['transition'] is not None
            if has_transition:
                event['notifications']['transition_email'] = event['body']['notifications']['transition']['email']
                event['notifications']['transition_sms'] = event['body']['notifications']['transition']['sms']
                event['notifications']['transition_push'] = event['body']['notifications']['transition']['push']

            has_at_risk = 'atRisk' in event['body']['notifications'] \
                          and event['body']['notifications']['atRisk'] is not None
            if has_at_risk:
                event['notifications']['at_risk_email'] = event['body']['notifications']['atRisk']['email']
                event['notifications']['at_risk_sms'] = event['body']['notifications']['atRisk']['sms']
                event['notifications']['at_risk_push'] = event['body']['notifications']['atRisk']['push']

            has_following = 'following' in event['body'] \
                            and event['body']['following'] is not None \
                            and len(event['body']['following']) > 0
            if has_following:
                event['following'] = []
                for f in event['body']['following']:
                    event['following'].append({
                        'user_id': f['id'],
                        'access_level': f['accessLevel'],
                        'given_name': f['givenName'],
                        'family_name': f['familyName'],
                        'role': f['role'],
                        'disease': f['disease']
                    })

            # mailing_address
            has_mailing_address = 'mailingAddress' in event['body'] \
                                  and event['body']['mailingAddress'] is not None
            if has_mailing_address:
                event['mailing_address'] = {
                    'street': event['body']['mailingAddress']['street'],
                    'city': event['body']['mailingAddress']['city'],
                    'country': event['body']['mailingAddress']['country'],
                }

                # check & add the optional fields for mailingAddresses globally
                has_postal_code = 'postalCode' in event['body']['mailingAddress'] \
                                  and event['body']['mailingAddress']['postalCode'] is not None
                if has_postal_code:
                    event['mailing_address']['postal_code'] = event['body']['mailingAddress']['postalCode']

                has_state_or_region = 'stateOrRegion' in event['body']['mailingAddress'] \
                                      and event['body']['mailingAddress']['stateOrRegion'] is not None
                if has_state_or_region:
                    event['mailing_address']['state_or_region'] = event['body']['mailingAddress']['stateOrRegion']

                has_street2 = 'street2' in event['body']['mailingAddress'] \
                              and event['body']['mailingAddress']['street2'] is not None
                if has_street2:
                    event['mailing_address']['street2'] = event['body']['mailingAddress']['street2']
                else:
                    event['mailing_address']['street2'] = ""

                has_state_code = 'stateCode' in event['body']['mailingAddress'] \
                                 and event['body']['mailingAddress']['stateCode'] is not None
                if has_state_code:
                    event['mailing_address']['state_code'] = event['body']['mailingAddress']['stateCode']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_caregiver', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_datalake_message_created(self, event):
        try:
            event['pubSubArgs'] = get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month
            if 'type' in event['body']:
                event['type'] = event['body']['type']
            if 'date' in event['body']:
                event['date'] = event['body']['date']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_message_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_external_id(self, event):
        try:
            event['pubSubArgs'] = get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['domain'] = event['body']['domain']
            event['key'] = event['body']['key']
            event['value'] = event['body']['value']
            event['cardinality'] = event['body']['cardinality']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month


            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_external_id', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_patient(self, event):
        try:
            event['pubSubArgs'] = get_pubSubArgs(event)
            has_role = 'role' in event['body'] and \
                       event['body']['role'] is not None
            if not has_role:

                print('role in map_patient: ' +event['body']['role'], file=sys.stdout)
                print('event in map_patient: ' + self.event_type, file=sys.stdout)
                print(
                    'non patient role sent to map_patient!',
                    file=sys.stdout
                )
                return event

            # there are 2 different major versions in play here
            # API 3.x & API 4.x. They are similar enough that we
            # just if'd it out. However, it might be wise to
            # somehow abstract this better. The switch is datalake
            # pubsub 1.7.0 version where we first started sending
            # API 4.x user data structures

            call_time = try_parsing_date(event['callTime'])
            date_created = try_parsing_date(event['body']['createdDate'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']

            if 'givenName' in event['body']:
                event['given_name'] = event['body']["givenName"]
            else:
                event['given_name'] = DEFAULT_NULL_STRING

            if 'timeZone' in event['body']:
                event['time_zone'] = event['body']["timeZone"]
            else:
                event['time_zone'] = DEFAULT_NULL_STRING

            if 'familyName' in event['body']:
                event['family_name'] = event['body']['familyName']
            else:
                event['family_name'] = DEFAULT_NULL_STRING

            if 'email' in event['body']:
                event['email'] = event['body']['email']
            else:
                event['email'] = DEFAULT_NULL_STRING

            if 'language' in event['body']:
                event['language'] = event['body']['language']
            else:
                event['language'] = DEFAULT_NULL_STRING

            if 'age' in event['body']:
                event['age'] = event['body']['age']
            else:
                event['age'] = DEFAULT_NULL_INTEGER

            if 'birthDate' in event['body']:
                event['birth_date'] = event['body']['birthDate']
            else:
                event['birth_date'] = DEFAULT_NULL_STRING

            if 'disease' in event['body']:
                event['disease'] = event['body']['disease']
            else:
                event['disease'] = DEFAULT_NULL_STRING

            if 'baseline' in event['body']:
                event['baseline'] = event['body']['baseline']
            else:
                event['baseline'] = DEFAULT_NULL_DOUBLE

            if 'role' in event['body']:
                event['role'] = event['body']['role']
            else:
                event['role'] = DEFAULT_NULL_STRING

            event['date_created'] = datetime_to_iso_str(date_created)
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            if 'group' in event['body']:
                event['group'] = {
                    'name': event['body']['group']['name'],
                    'display_name': event['body']['group']['displayName']
                }
            else:
                event['group'] = {
                    'name': DEFAULT_NULL_STRING,
                    'display_name': DEFAULT_NULL_STRING
                }

            has_phone = 'phone' in event['body'] \
                        and event['body']['phone'] is not None
            event['has_phone'] = has_phone
            if has_phone:
                event['phone'] = str(event['body']['phone'])
            else:
                event['phone'] = DEFAULT_NULL_STRING

            # followers
            has_followers = 'followers' in event['body'] \
                            and event['body']['followers'] is not None \
                            and len(event['body']['followers']) > 0
            if has_followers:
                event['followers'] = []
                for f in event['body']['followers']:
                    f2 = {}
                    has_user_id = 'id' in f \
                                  and f['id'] is not None
                    f2['has_user_id'] = has_user_id
                    if has_user_id:
                        f2['user_id'] = f['id']
                    else:
                        f2['user_id'] = DEFAULT_NULL_STRING

                    has_access_level = 'accessLevel' in f \
                                       and f['accessLevel'] is not None
                    f2['has_access_level'] = has_access_level
                    if has_access_level:
                        f2['access_level'] = f['accessLevel']
                    else:
                        f2['access_level'] = DEFAULT_NULL_STRING

                    has_given_name = 'givenName' in f \
                                     and f['givenName'] is not None
                    f2['has_given_name'] = has_given_name
                    if has_given_name:
                        f2['given_name'] = f['givenName']
                    else:
                        f2['given_name'] = DEFAULT_NULL_STRING

                    has_family_name = 'familyName' in f \
                                      and f['familyName'] is not None
                    f2['has_family_name'] = has_family_name
                    if has_family_name:
                        f2['family_name'] = f['familyName']
                    else:
                        f2['family_name'] = DEFAULT_NULL_STRING

                    has_role = 'role' in f \
                               and f['role'] is not None
                    f2['has_role'] = has_role
                    if has_role:
                        f2['role'] = f['role']
                    else:
                        f2['role'] = DEFAULT_NULL_STRING

                    event['followers'].append(f2)
            else:
                event['followers'] = [{
                    'role': DEFAULT_NULL_STRING,
                    'access_level': DEFAULT_NULL_STRING,
                    'has_access_level': DEFAULT_NULL_BOOLEAN,
                    'has_user_id': DEFAULT_NULL_BOOLEAN,
                    'has_family_name': DEFAULT_NULL_BOOLEAN,
                    'has_role': DEFAULT_NULL_BOOLEAN,
                    'has_given_name': DEFAULT_NULL_BOOLEAN,
                    'given_name': DEFAULT_NULL_STRING,
                    'family_name': DEFAULT_NULL_STRING,
                    'user_id': DEFAULT_NULL_STRING
                }]


            # following
            has_following = 'following' in event['body'] \
                            and event['body']['following'] is not None \
                            and len(event['body']['following']) > 0
            if has_following:
                event['following'] = []
                for f in event['body']['following']:
                    f2 = {}
                    has_user_id = 'id' in f \
                                  and f['id'] is not None
                    f2['has_user_id'] = has_user_id
                    if has_user_id:
                        f2['user_id'] = f['id']
                    else:
                        f2['user_id'] = DEFAULT_NULL_STRING

                    has_access_level = 'accessLevel' in f \
                                       and f['accessLevel'] is not None
                    f2['has_access_level'] = has_access_level
                    if has_access_level:
                        f2['access_level'] = f['accessLevel']
                    else:
                        f2['access_level'] = DEFAULT_NULL_STRING

                    has_given_name = 'givenName' in f \
                                     and f['givenName'] is not None
                    f2['has_given_name'] = has_given_name
                    if has_given_name:
                        f2['given_name'] = f['givenName']
                    else:
                        f2['given_name'] = DEFAULT_NULL_STRING

                    has_family_name = 'familyName' in f \
                                      and f['familyName'] is not None
                    f2['has_family_name'] = has_family_name
                    if has_family_name:
                        f2['family_name'] = f['familyName']
                    else:
                        f2['family_name'] = DEFAULT_NULL_STRING

                    has_role = 'role' in f \
                               and f['role'] is not None
                    f2['has_role'] = has_role
                    if has_role:
                        f2['role'] = f['role']
                    else:
                        f2['role'] = DEFAULT_NULL_STRING

                    has_disease = 'disease' in f \
                                  and f['disease'] is not None
                    f2['has_disease'] = has_disease
                    if has_disease:
                        f2['disease'] = f['disease']
                    else:
                        f2['disease'] = DEFAULT_NULL_STRING

                    event['following'].append(f2)
            else:
                event['following'] = [{
                    'role': DEFAULT_NULL_STRING,
                    'access_level': DEFAULT_NULL_STRING,
                    'has_access_level': DEFAULT_NULL_BOOLEAN,
                    'has_disease': DEFAULT_NULL_BOOLEAN,
                    'has_family_name': DEFAULT_NULL_BOOLEAN,
                    'has_user_id': DEFAULT_NULL_BOOLEAN,
                    'has_role': DEFAULT_NULL_BOOLEAN,
                    'has_given_name': DEFAULT_NULL_BOOLEAN,
                    'disease': DEFAULT_NULL_STRING,
                    'given_name': DEFAULT_NULL_STRING,
                    'family_name': DEFAULT_NULL_STRING,
                    'user_id': DEFAULT_NULL_STRING
                }]

            # optional notifications
            has_notifications = 'notifications' in event['body'] \
                                and event['body']['notifications'] is not None
            if has_notifications:
                event['notifications'] = {}
                has_digest = 'digest' in event['body']['notifications'] \
                             and event['body']['notifications']['digest'] is not None
                event['notifications']['has_digest'] = has_digest
                if has_digest:
                    event['notifications']['digest_email'] = event['body']['notifications']['digest']['email']
                else:
                    event['notifications']['digest_email'] = DEFAULT_NULL_BOOLEAN

                has_notes = 'notes' in event['body']['notifications'] \
                            and event['body']['notifications']['notes'] is not None
                event['notifications']['has_notes'] = has_notes
                if has_notes:
                    event['notifications']['notes_email'] = event['body']['notifications']['notes']['email']
                    event['notifications']['notes_sms'] = event['body']['notifications']['notes']['sms']
                    event['notifications']['notes_push'] = event['body']['notifications']['notes']['push']
                else:
                    event['notifications']['notes_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['notes_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['notes_push'] = DEFAULT_NULL_BOOLEAN

                has_missed_dose = 'missedDose' in event['body']['notifications'] \
                                  and event['body']['notifications']['missedDose'] is not None
                event['body']['notifications']['has_missed_dose'] = has_missed_dose
                if has_missed_dose:
                    event['notifications']['missed_dose_email'] = event['body']['notifications']['missedDose']['email']
                    event['notifications']['missed_dose_sms'] = event['body']['notifications']['missedDose']['sms']
                    event['notifications']['missed_dose_push'] = event['body']['notifications']['missedDose']['push']
                else:
                    event['notifications']['missed_dose_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['missed_dose_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['missed_dose_push'] = DEFAULT_NULL_BOOLEAN

                has_rescue_usage = 'rescueUsage' in event['body']['notifications'] \
                                   and event['body']['notifications']['rescueUsage'] is not None
                event['notifications']['has_rescue_usage'] = has_rescue_usage
                if has_rescue_usage:
                    event['notifications']['rescue_usage_email'] = event['body']['notifications']['rescueUsage']['email']
                    event['notifications']['rescue_usage_sms'] = event['body']['notifications']['rescueUsage']['sms']
                    event['notifications']['rescue_usage_push'] = event['body']['notifications']['rescueUsage']['push']
                else:
                    event['notifications']['rescue_usage_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['rescue_usage_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['rescue_usage_push'] = DEFAULT_NULL_BOOLEAN

                has_goal = 'goal' in event['body']['notifications'] \
                           and event['body']['notifications']['goal'] is not None
                event['notifications']['has_goal'] = has_goal
                if has_goal:
                    event['notifications']['goal_email'] = event['body']['notifications']['goal']['email']
                    event['notifications']['goal_sms'] = event['body']['notifications']['goal']['sms']
                    event['notifications']['goal_push'] = event['body']['notifications']['goal']['push']
                else:
                    event['notifications']['goal_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['goal_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['goal_push'] = DEFAULT_NULL_BOOLEAN

                has_quiet_sensor = 'quietSensor' in event['body']['notifications'] \
                                   and event['body']['notifications']['quietSensor'] is not None
                event['notifications']['has_quiet_sensor'] = has_quiet_sensor
                if has_quiet_sensor:
                    event['notifications']['quiet_sensor_email'] = event['body']['notifications']['quietSensor']['email']
                    event['notifications']['quiet_sensor_sms'] = event['body']['notifications']['quietSensor']['sms']
                    event['notifications']['quiet_sensor_push'] = event['body']['notifications']['quietSensor']['push']
                else:
                    event['notifications']['quiet_sensor_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['quiet_sensor_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['quiet_sensor_push'] = DEFAULT_NULL_BOOLEAN

                has_act_invite = 'actInvite' in event['body']['notifications'] \
                                 and event['body']['notifications']['actInvite'] is not None
                event['notifications']['has_act_invite'] = has_act_invite
                if has_act_invite:
                    event['notifications']['act_invite_email'] = event['body']['notifications']['actInvite']['email']
                    event['notifications']['act_invite_sms'] = event['body']['notifications']['actInvite']['sms']
                    event['notifications']['act_invite_push'] = event['body']['notifications']['actInvite']['push']
                else:
                    # API 4.x renamed to questionnaire
                    has_act_invite = 'questionnaire' in event['body']['notifications'] \
                                     and event['body']['notifications']['questionnaire'] is not None
                    event['notifications']['has_act_invite'] = has_act_invite
                    if has_act_invite:
                        event['notifications']['act_invite_email'] = event['body']['notifications']['questionnaire']['email']
                        event['notifications']['act_invite_sms'] = event['body']['notifications']['questionnaire']['sms']
                        event['notifications']['act_invite_push'] = event['body']['notifications']['questionnaire']['push']
                    else:
                        event['notifications']['act_invite_email'] = DEFAULT_NULL_BOOLEAN
                        event['notifications']['act_invite_sms'] = DEFAULT_NULL_BOOLEAN
                        event['notifications']['act_invite_push'] = DEFAULT_NULL_BOOLEAN

                has_transition = 'transition' in event['body']['notifications'] \
                                 and event['body']['notifications']['transition'] is not None
                event['notifications']['has_transition'] = has_transition
                if has_transition:
                    event['notifications']['transition_email'] = event['body']['notifications']['transition']['email']
                    event['notifications']['transition_sms'] = event['body']['notifications']['transition']['sms']
                    event['notifications']['transition_push'] = event['body']['notifications']['transition']['push']
                else:
                    event['notifications']['transition_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['transition_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['transition_push'] = DEFAULT_NULL_BOOLEAN

                has_environmental = 'environmental' in event['body']['notifications'] \
                                    and event['body']['notifications']['environmental'] is not None
                event['notifications']['has_environmental'] = has_environmental
                if has_environmental:
                    event['notifications']['environmental_email'] = event['body']['notifications']['environmental']['email']
                    event['notifications']['environmental_sms'] = event['body']['notifications']['environmental']['sms']
                    event['notifications']['environmental_push'] = event['body']['notifications']['environmental']['push']
                else:
                    event['notifications']['environmental_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['environmental_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['environmental_push'] = DEFAULT_NULL_BOOLEAN

                has_at_risk = 'atRisk' in event['body']['notifications'] \
                              and event['body']['notifications']['atRisk'] is not None
                event['notifications']['has_at_risk'] = has_at_risk
                if has_at_risk:
                    event['notifications']['at_risk_email'] = event['body']['notifications']['atRisk']['email']
                    event['notifications']['at_risk_sms'] = event['body']['notifications']['atRisk']['sms']
                    event['notifications']['at_risk_push'] = event['body']['notifications']['atRisk']['push']
                else:
                    event['notifications']['at_risk_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['at_risk_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['at_risk_push'] = DEFAULT_NULL_BOOLEAN
            else:
                event['notifications']  = {
                    'transition_sms': DEFAULT_NULL_BOOLEAN,
                    'transition_email': DEFAULT_NULL_BOOLEAN,
                    'goal_email': DEFAULT_NULL_BOOLEAN,
                    'has_goal': DEFAULT_NULL_BOOLEAN,
                    'notes_push': DEFAULT_NULL_BOOLEAN,
                    'quiet_sensor_push': DEFAULT_NULL_BOOLEAN,
                    'goal_sms': DEFAULT_NULL_BOOLEAN,
                    'goal_push': DEFAULT_NULL_BOOLEAN,
                    'has_rescue_usage': DEFAULT_NULL_BOOLEAN,
                    'at_risk_email': DEFAULT_NULL_BOOLEAN,
                    'quiet_sensor_sms': DEFAULT_NULL_BOOLEAN,
                    'missed_dose_email': DEFAULT_NULL_BOOLEAN,
                    'at_risk_push': DEFAULT_NULL_BOOLEAN,
                    'has_notes': DEFAULT_NULL_BOOLEAN,
                    'quiet_sensor_email': DEFAULT_NULL_BOOLEAN,
                    'transition_push': DEFAULT_NULL_BOOLEAN,
                    'at_risk_sms': DEFAULT_NULL_BOOLEAN,
                    'rescue_usage_email': DEFAULT_NULL_BOOLEAN,
                    'environmental_push': DEFAULT_NULL_BOOLEAN,
                    'missed_dose_sms': DEFAULT_NULL_BOOLEAN,
                    'act_invite_sms': DEFAULT_NULL_BOOLEAN,
                    'act_invite_email': DEFAULT_NULL_BOOLEAN,
                    'notes_sms': DEFAULT_NULL_BOOLEAN,
                    'act_invite_push': DEFAULT_NULL_BOOLEAN,
                    'missed_dose_push': DEFAULT_NULL_BOOLEAN,
                    'rescue_usage_sms': DEFAULT_NULL_BOOLEAN,
                    'rescue_usage_push': DEFAULT_NULL_BOOLEAN,
                    'environmental_email': DEFAULT_NULL_BOOLEAN,
                    'digest_email': DEFAULT_NULL_BOOLEAN,
                    'has_transition': DEFAULT_NULL_BOOLEAN,
                    'has_act_invite': DEFAULT_NULL_BOOLEAN,
                    'notes_email': DEFAULT_NULL_BOOLEAN,
                    'has_environmental': DEFAULT_NULL_BOOLEAN,
                    'has_quiet_sensor': DEFAULT_NULL_BOOLEAN,
                    'has_at_risk': DEFAULT_NULL_BOOLEAN,
                    'environmental_sms': DEFAULT_NULL_BOOLEAN,
                    'has_digest': DEFAULT_NULL_BOOLEAN
                }
            # medications
            has_medications = 'plan' in event['body'] \
                              and event['body']['plan'] is not None \
                              and 'medications' in event['body']['plan'] \
                              and event['body']['plan']['medications'] is not None \
                              and len(event['body']['plan']['medications']) > 0
            event['has_medications'] = has_medications
            event['usage_list'] = []
            event['sensors'] = []
            if has_medications:
                event['medications'] = []
                for m in event['body']['plan']['medications']:
                    medication = {
                        'medication_id': m['medication']['id']
                    }
                    has_data_link = 'dataLink' in m \
                                    and m['dataLink'] is not None
                    medication['has_data_link'] = has_data_link
                    if has_data_link:
                        medication['data_link'] = m['dataLink']
                    else:
                        medication['data_link'] = DEFAULT_NULL_BOOLEAN

                    # API 4.x periods
                    has_effective_period = 'effectivePeriod' in m \
                                           and m['effectivePeriod'] is not None
                    medication['has_effective_period_start'] = has_effective_period
                    if has_effective_period:
                        medication['effective_period_start'] = m['effectivePeriod']['start']
                        has_effective_period_end = 'end' in m['effectivePeriod'] and \
                                                   m['effectivePeriod']['end'] is not None
                        medication['has_effective_period_end'] = has_effective_period
                        if has_effective_period_end:
                            medication['effective_period_end'] = m['effectivePeriod']['end']
                        else:
                            medication['effective_period_end'] = DEFAULT_NULL_DATETIME
                    else:
                        medication['has_effective_period_end'] = has_effective_period
                        medication['effective_period_start'] = DEFAULT_NULL_DATETIME
                        medication['effective_period_end'] = DEFAULT_NULL_DATETIME

                    # API 4.x status
                    has_status = 'status' in m and \
                                 m['status'] is not None
                    medication['has_status'] = has_status
                    if has_status:
                        medication['status'] = m['status']
                    else:
                        medication['status'] = DEFAULT_NULL_STRING

                    event['medications'].append(medication)

                    has_usage_list = 'usageList' in m \
                                     and m['usageList'] is not None \
                                     and len(m['usageList']) > 0
                    if has_usage_list:
                        for ul in m['usageList']:
                            dose_time = {
                                'medication_id': m['medication']['id'],
                                'hour': ul['hour'],
                                'minute': ul['minute']
                            }
                            has_doses = 'doses' in ul and \
                                        ul['doses'] is not None
                            if has_doses:
                                dose_time['doses'] = ul['doses']
                            else:
                                # API 4.x updated to unitDoses
                                has_unit_doses = 'unitDoses' in ul and \
                                                 ul['unitDoses'] is not None
                                if has_unit_doses:
                                    dose_time['doses'] = ul['unitDoses']

                            event['usage_list'].append(dose_time)

                    has_sensors = 'sensors' in m \
                                  and m['sensors'] is not None \
                                  and len(m['sensors']) > 0
                    if has_sensors:
                        for s in m['sensors']:
                            sensor = {
                                'mac': s['mac'],
                                'medication_id': m['medication']['id']
                            }

                            # add medication type
                            has_medication_type = 'type' in m['medication'] \
                                                  and m['medication']['type'] is not None
                            sensor['has_medication_type'] = has_medication_type
                            if has_medication_type:
                                sensor['medication_type'] = m['medication']['type']
                            else:
                                sensor['medication_type'] = DEFAULT_NULL_STRING

                            # optional sensor fields
                            has_battery = 'battery' in s \
                                          and s['battery'] is not None
                            sensor['has_battery'] = has_battery
                            if has_battery:
                                sensor['battery'] = s['battery']
                            else:
                                # battery renamed to batteryLevel in 4.x
                                has_battery = 'batteryLevel' in s \
                                              and s['batteryLevel'] is not None
                                sensor['has_battery'] = has_battery
                                if has_battery:
                                    sensor['battery'] = s['batteryLevel']
                                else:
                                    sensor['battery'] = DEFAULT_NULL_INTEGER

                            has_data_link = 'dataLink' in s \
                                            and s['dataLink'] is not None
                            sensor['has_data_link'] = has_data_link
                            if has_data_link:
                                sensor['data_link'] = s['dataLink']
                            else:
                                sensor['data_link'] = DEFAULT_NULL_BOOLEAN

                            has_first_sync_date = 'firstSyncDate' in s \
                                                  and s['firstSyncDate'] is not None
                            sensor['has_first_sync_date'] = has_first_sync_date
                            if has_first_sync_date:
                                sensor['first_sync_date'] = s['firstSyncDate']
                            else:
                                sensor['first_sync_date'] = DEFAULT_NULL_DATETIME

                            has_firmware_version = 'firmwareVersion' in s \
                                                   and s['firmwareVersion'] is not None
                            sensor['has_firmware_version'] = has_firmware_version
                            if has_firmware_version:
                                sensor['firmware_version'] = s['firmwareVersion']
                            else:
                                sensor['firmware_version'] = DEFAULT_NULL_STRING

                            has_force_silent = 'forceSilent' in s \
                                               and s['forceSilent'] is not None
                            sensor['has_force_silent'] = has_force_silent
                            if has_force_silent:
                                sensor['force_silent'] = s['forceSilent']
                            else:
                                sensor['force_silent'] = DEFAULT_NULL_BOOLEAN

                            has_last_sync_date = 'lastSyncDate' in s \
                                                 and s['lastSyncDate'] is not None
                            sensor['has_last_sync_date'] = has_last_sync_date
                            if has_last_sync_date:
                                sensor['last_sync_date'] = s['lastSyncDate']
                            else:
                                sensor['last_sync_date'] = DEFAULT_NULL_DATETIME

                            has_model = 'model' in s and s['model'] is not None
                            sensor['has_model'] = has_model
                            if has_model:
                                sensor['model'] = s['model']
                            else:
                                sensor['model'] = DEFAULT_NULL_STRING

                            has_ready_date = 'readyDate' in s \
                                             and s['readyDate'] is not None
                            sensor['has_ready_date'] = has_ready_date
                            if has_ready_date:
                                sensor['ready_date'] = s['readyDate']
                            else:
                                sensor['ready_date'] = DEFAULT_NULL_DATETIME

                            has_silent = 'silent' in s \
                                         and s['silent'] is not None
                            sensor['has_silent'] = has_silent
                            if has_silent:
                                sensor['silent'] = s['silent']
                            else:
                                sensor['silent'] = DEFAULT_NULL_BOOLEAN

                            event['sensors'].append(sensor)
            else:
                event['medications'] = [{
                    'has_status': DEFAULT_NULL_BOOLEAN,
                    'has_effective_period_end': DEFAULT_NULL_BOOLEAN,
                    'has_effective_period_start': DEFAULT_NULL_BOOLEAN,
                    'effective_period_end': DEFAULT_NULL_DATETIME,
                    'data_link': DEFAULT_NULL_BOOLEAN,
                    'has_data_link': DEFAULT_NULL_BOOLEAN,
                    'status': DEFAULT_NULL_STRING,
                    'medication_id': DEFAULT_NULL_STRING,
                    'effective_period_start': DEFAULT_NULL_DATETIME
                }]
            has_usage_list = len(event['usage_list']) > 0
            has_sensors = len(event['sensors']) > 0
            event['has_usage_list'] = has_usage_list
            event['has_sensors'] = has_sensors

            if not has_usage_list:
                event['usage_list'].append({
                    'doses': DEFAULT_NULL_INTEGER,
                    'hour': DEFAULT_NULL_INTEGER,
                    'medication_id': DEFAULT_NULL_STRING,
                    'minute': DEFAULT_NULL_INTEGER
                })

            if not has_sensors:
                event['sensors'].append({
                    'has_silent': DEFAULT_NULL_BOOLEAN,
                    'has_model': DEFAULT_NULL_BOOLEAN,
                    'medication_type': DEFAULT_NULL_STRING,
                    'force_silent': DEFAULT_NULL_BOOLEAN,
                    'has_ready_date': DEFAULT_NULL_BOOLEAN,
                    'mac': DEFAULT_NULL_STRING,
                    'has_firmware_version': DEFAULT_NULL_BOOLEAN,
                    'model': DEFAULT_NULL_STRING,
                    'ready_date': DEFAULT_NULL_DATETIME,
                    'last_sync_date': DEFAULT_NULL_DATETIME,
                    'has_force_silent': DEFAULT_NULL_BOOLEAN,
                    'has_first_sync_date': DEFAULT_NULL_BOOLEAN,
                    'silent': DEFAULT_NULL_BOOLEAN,
                    'has_medication_type': DEFAULT_NULL_BOOLEAN,
                    'data_link': DEFAULT_NULL_BOOLEAN,
                    'has_battery': DEFAULT_NULL_BOOLEAN,
                    'has_data_link': DEFAULT_NULL_BOOLEAN,
                    'medication_id': DEFAULT_NULL_STRING,
                    'battery': DEFAULT_NULL_INTEGER,
                    'first_sync_date': DEFAULT_NULL_DATETIME,
                    'has_last_sync_date': DEFAULT_NULL_BOOLEAN,
                    'firmware_version': DEFAULT_NULL_STRING
                })

            # mailing_address
            has_mailing_address = 'mailingAddress' in event['body'] \
                                  and event['body']['mailingAddress'] is not None
            if has_mailing_address:
                event['mailing_address'] = {
                    'street': event['body']['mailingAddress']['street'],
                    'city': event['body']['mailingAddress']['city'],
                    'country': event['body']['mailingAddress']['country'],
                }

                # check & add the optional fields for mailingAddresses globally
                has_postal_code = 'postalCode' in event['body']['mailingAddress'] \
                                  and event['body']['mailingAddress']['postalCode'] is not None
                event['mailing_address']['has_postal_code'] = has_postal_code
                if has_postal_code:
                    event['mailing_address']['postal_code'] = event['body']['mailingAddress']['postalCode']
                else:
                    event['mailing_address']['postal_code'] = DEFAULT_NULL_STRING

                has_state_or_region = 'stateOrRegion' in event['body']['mailingAddress'] \
                                      and event['body']['mailingAddress']['stateOrRegion'] is not None
                event['mailing_address']['has_state_or_region'] = has_state_or_region
                if has_state_or_region:
                    event['mailing_address']['state_or_region'] = event['body']['mailingAddress']['stateOrRegion']
                else:
                    event['mailing_address']['state_or_region'] = DEFAULT_NULL_STRING

                has_street2 = 'street2' in event['body']['mailingAddress'] \
                              and event['body']['mailingAddress']['street2'] is not None
                event['mailing_address']['has_street2'] = has_street2
                if has_street2:
                    event['mailing_address']['street2'] = event['body']['mailingAddress']['street2']
                else:
                    event['mailing_address']['street2'] = DEFAULT_NULL_STRING

                has_state_code = 'stateCode' in event['body']['mailingAddress'] \
                                 and event['body']['mailingAddress']['stateCode'] is not None
                event['mailing_address']['has_state_code'] = has_state_code
                if has_state_code:
                    event['mailing_address']['state_code'] = event['body']['mailingAddress']['stateCode']
                else:
                    event['mailing_address']['state_code'] = DEFAULT_NULL_STRING
            else:
                event['mailing_address'] = {
                    'state_code': DEFAULT_NULL_STRING,
                    'has_street2': DEFAULT_NULL_BOOLEAN,
                    'city': DEFAULT_NULL_STRING,
                    'country': DEFAULT_NULL_STRING,
                    'street2': DEFAULT_NULL_STRING,
                    'has_state_or_region': DEFAULT_NULL_BOOLEAN,
                    'has_postal_code': DEFAULT_NULL_BOOLEAN,
                    'postal_code': DEFAULT_NULL_STRING,
                    'has_state_code': DEFAULT_NULL_BOOLEAN,
                    'street': DEFAULT_NULL_STRING,
                    'state_or_region': DEFAULT_NULL_STRING
                }

            # gender
            has_gender = 'gender' in event['body'] \
                         and event['body']['gender'] is not None
            event['has_gender'] = has_gender
            if has_gender:
                event['gender'] = event['body']['gender']
            else:
                event['gender'] = DEFAULT_NULL_STRING

            # height
            has_height = 'height' in event['body'] \
                         and event['body']['height'] is not None
            event['has_height'] = has_height
            if has_height:
                if isinstance(event['body']['height'], dict):
                    # API 4.x is a struct
                    event['height'] = float(event['body']['height']['value'])
                else:
                    event['height'] = float(event['body']['height'])
            else:
                event['height'] = DEFAULT_NULL_DOUBLE

            # medical_ids
            has_medical_ids = 'medicalIds' in event['body'] \
                              and event['body']['medicalIds'] is not None \
                              and len(event['body']['medicalIds']) > 0
            event['has_medical_ids'] = has_medical_ids
            if has_medical_ids:
                event['medical_ids'] = []
                for m in event['body']['medicalIds']:
                    event['medical_ids'].append({
                        'key': m['key'],
                        'value': m['value']
                    })
            else:
                # API 4.x renamed
                has_medical_ids = 'externalIds' in event['body'] \
                                  and event['body']['externalIds'] is not None \
                                  and len(event['body']['externalIds']) > 0
                event['has_medical_ids'] = has_medical_ids
                if has_medical_ids:
                    event['medical_ids'] = []
                    for m in event['body']['externalIds']:
                        event['medical_ids'].append({
                            'key': m['key'],
                            'value': m['value']
                        })
                else:
                    event['medical_ids'] = [{
                        'key': DEFAULT_NULL_STRING,
                        'value': DEFAULT_NULL_STRING
                    }]

            # hub
            has_hub = 'hub' in event['body'] \
                      and event['body']['hub'] is not None
            event['has_hub'] = has_hub
            if has_hub:
                event['hub'] = {}
                has_id = 'id' in event['body']['hub'] \
                         and event['body']['hub']['id'] is not None
                event['hub']['has_id'] = has_id
                if has_id:
                    event['hub']['id'] = event['body']['hub']['id']
                else:
                    event['hub']['id'] = DEFAULT_NULL_STRING

                has_external_id = 'externalId' in event['body']['hub'] \
                                  and event['body']['hub']['externalId'] is not None
                event['hub']['has_external_id'] = has_external_id
                if has_external_id:
                    event['hub']['external_id'] = event['body']['hub']['externalId']
                else:
                    event['hub']['external_id'] = DEFAULT_NULL_STRING

                has_type = 'type' in event['body']['hub'] \
                           and event['body']['hub']['type'] is not None
                event['hub']['has_type'] = has_type
                if has_type:
                    event['hub']['type'] = event['body']['hub']['type']
                else:
                    event['hub']['type'] = DEFAULT_NULL_STRING

                has_region = 'region' in event['body']['hub'] \
                             and event['body']['hub']['region'] is not None
                event['hub']['has_region'] = has_region
                if has_region:
                    event['hub']['region'] = event['body']['hub']['region']
                else:
                    event['hub']['region'] = DEFAULT_NULL_STRING
            else:
                event['hub'] = {
                    'type': DEFAULT_NULL_STRING,
                    'has_id': DEFAULT_NULL_BOOLEAN,
                    'region': DEFAULT_NULL_STRING,
                    'has_region': DEFAULT_NULL_BOOLEAN,
                    'has_external_id': DEFAULT_NULL_BOOLEAN,
                    'external_id': DEFAULT_NULL_STRING,
                    'id': DEFAULT_NULL_STRING,
                    'has_type': DEFAULT_NULL_BOOLEAN
                }

            # weight
            has_weight = 'weight' in event['body'] \
                         and event['body']['weight'] is not None
            event['has_weight'] = has_weight
            if has_weight:
                if isinstance(event['body']['weight'], dict):
                    # API 4.x is a struct
                    event['weight'] = float(event['body']['weight']['value'])
                else:
                    event['weight'] = float(event['body']['weight'])
            else:
                event['weight'] = DEFAULT_NULL_DOUBLE

            # race
            has_race = 'race' in event['body'] \
                       and event['body']['race'] is not None
            event['has_race'] = has_race
            if has_race:
                event['race'] = event['body']['race']
            else:
                event['race'] = DEFAULT_NULL_STRING

            # clinical trial
            has_clinical_trial = 'clinicalTrial' in event['body'] \
                                 and event['body']['clinicalTrial'] is not None
            event['has_clinical_trial'] = has_clinical_trial
            if has_clinical_trial:
                event['clinical_trial'] = {}
                has_clinical_trial_id = 'clinicalTrialId' in event['body']['clinicalTrial'] \
                                        and event['body']['clinicalTrial']['clinicalTrialId'] is not None
                event['clinical_trial']['has_id'] = has_clinical_trial_id
                if has_clinical_trial_id:
                    event['clinical_trial']['id'] = event['body']['clinicalTrial']['clinicalTrialId']
                else:
                    event['clinical_trial']['id'] = DEFAULT_NULL_STRING

                has_arm_id = 'clinicalTrialArmId' in event['body']['clinicalTrial'] \
                             and event['body']['clinicalTrial']['clinicalTrialArmId'] is not None
                event['clinical_trial']['has_arm_id'] = has_arm_id
                if has_arm_id:
                    event['clinical_trial']['arm_id'] = event['body']['clinicalTrial']['clinicalTrialArmId']
                else:
                    event['clinical_trial']['arm_id'] = DEFAULT_NULL_STRING

                has_clinical_trial_status = 'status' in event['body']['clinicalTrial'] \
                                            and event['body']['clinicalTrial']['status'] is not None
                event['clinical_trial']['has_status'] = has_clinical_trial_status
                if has_clinical_trial_status:
                    event['clinical_trial']['status'] = event['body']['clinicalTrial']['status']
                else:
                    event['clinical_trial']['status'] = DEFAULT_NULL_STRING
            else:
                event['clinical_trial'] = {
                    'has_status': DEFAULT_NULL_BOOLEAN,
                    'arm_id': DEFAULT_NULL_STRING,
                    'status': DEFAULT_NULL_STRING,
                    'has_id': DEFAULT_NULL_BOOLEAN,
                    'has_arm_id': DEFAULT_NULL_BOOLEAN,
                    'id': DEFAULT_NULL_STRING
                }

            has_event = 'event' in event['body'] \
                        and event['body']['event'] is not None
            event['has_event'] = has_event
            if has_event:
                event['event'] = {}
                has_first = 'first' in event['body']['event'] \
                            and event['body']['event']['first'] is not None
                has_first_controller = 'firstController' in event['body']['event'] \
                                       and event['body']['event']['firstController'] is not None
                has_first_rescue = 'firstRescue' in event['body']['event'] \
                                   and event['body']['event']['firstRescue'] is not None
                has_last = 'last' in event['body']['event'] \
                           and event['body']['event']['last'] is not None
                has_last_controller = 'lastController' in event['body']['event'] \
                                      and event['body']['event']['lastController'] is not None
                has_last_rescue = 'lastRescue' in event['body']['event'] \
                                  and event['body']['event']['lastRescue'] is not None
                has_count_rescue = 'totalRescue' in event['body']['event'] \
                                   and event['body']['event']['totalRescue'] is not None

                # set the has_* fields
                event['event']['has_first'] = has_first
                event['event']['has_first_controller'] = has_first_controller
                event['event']['has_first_rescue'] = has_first_rescue
                event['event']['has_last'] = has_last
                event['event']['has_last_controller'] = has_last_controller
                event['event']['has_last_rescue'] = has_last_rescue
                event['event']['has_count_rescue'] = has_count_rescue

                # set the various fields or default
                if has_first:
                    event['event']['first'] = event['body']['event']['first']
                else:
                    event['event']['first'] = DEFAULT_NULL_DATETIME

                if has_first_controller:
                    event['event']['first_controller'] = event['body']['event']['firstController']
                else:
                    event['event']['first_controller'] = DEFAULT_NULL_DATETIME

                if has_first_rescue:
                    event['event']['first_rescue'] = event['body']['event']['firstRescue']
                else:
                    event['event']['first_rescue'] = DEFAULT_NULL_DATETIME

                if has_last:
                    event['event']['last'] = event['body']['event']['last']
                else:
                    event['event']['last'] = DEFAULT_NULL_DATETIME

                if has_last_controller:
                    event['event']['last_controller'] = event['body']['event']['lastController']
                else:
                    event['event']['last_controller'] = DEFAULT_NULL_DATETIME

                if has_last_rescue:
                    event['event']['last_rescue'] = event['body']['event']['lastRescue']
                else:
                    event['event']['last_rescue'] = DEFAULT_NULL_DATETIME

                if has_count_rescue:
                    event['event']['count_rescue'] = int(event['body']['event']['totalRescue'])
                else:
                    event['event']['count_rescue'] = DEFAULT_NULL_INTEGER
            else:
                event['event'] = {
                    'first_controller': DEFAULT_NULL_DATETIME,
                    'has_last': DEFAULT_NULL_BOOLEAN,
                    'first_rescue': DEFAULT_NULL_DATETIME,
                    'last_controller': DEFAULT_NULL_DATETIME,
                    'has_last_rescue': DEFAULT_NULL_BOOLEAN,
                    'last_rescue': DEFAULT_NULL_DATETIME,
                    'last': DEFAULT_NULL_DATETIME,
                    'has_first_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first_rescue': DEFAULT_NULL_BOOLEAN,
                    'count_rescue': DEFAULT_NULL_INTEGER,
                    'has_last_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first': DEFAULT_NULL_BOOLEAN,
                    'has_count_rescue': DEFAULT_NULL_BOOLEAN,
                    'first': DEFAULT_NULL_DATETIME
                }

            has_sync = 'sync' in event['body'] \
                       and event['body']['sync'] is not None
            event['has_sync'] = has_sync
            if has_sync:
                event['sync'] = {}
                has_first = 'first' in event['body']['sync'] \
                            and event['body']['sync'] is not None
                has_first_controller = 'firstController' in event['body']['sync'] \
                                       and event['body']['sync']['firstController'] is not None
                has_first_rescue = 'firstRescue' in event['body']['sync'] \
                                   and event['body']['sync']['firstRescue'] is not None
                has_last = 'last' in event['body']['sync'] \
                           and event['body']['sync']['last'] is not None
                has_last_controller = 'lastController' in event['body']['sync'] \
                                      and event['body']['sync']['lastController'] is not None
                has_last_rescue = 'lastRescue' in event['body']['sync'] \
                                  and event['body']['sync']['lastRescue'] is not None
                has_count = 'sync' in event['body']['sync'] \
                            and event['body']['sync']['sync'] is not None

                event['sync']['has_first'] = has_first
                event['sync']['has_first_controller'] = has_first_controller
                event['sync']['has_first_rescue'] = has_first_rescue
                event['sync']['has_last'] = has_last
                event['sync']['has_last_controller'] = has_last_controller
                event['sync']['has_last_rescue'] = has_last_rescue
                event['sync']['has_count'] = has_count

                if has_first:
                    event['sync']['first'] = event['body']['sync']['first']
                else:
                    event['sync']['first'] = DEFAULT_NULL_DATETIME

                if has_first_controller:
                    event['sync']['first_controller'] = event['body']['sync']['firstController']
                else:
                    event['sync']['first_controller'] = DEFAULT_NULL_DATETIME

                if has_first_rescue:
                    event['sync']['first_rescue'] = event['body']['sync']['firstRescue']
                else:
                    event['sync']['first_rescue'] = DEFAULT_NULL_DATETIME

                if has_last:
                    event['sync']['last'] = event['body']['sync']['last']
                else:
                    event['sync']['last'] = DEFAULT_NULL_DATETIME

                if has_last_controller:
                    event['sync']['last_controller'] = event['body']['sync']['lastController']
                else:
                    event['sync']['last_controller'] = DEFAULT_NULL_DATETIME

                if has_last_rescue:
                    event['sync']['last_rescue'] = event['body']['sync']['lastRescue']
                else:
                    event['sync']['last_rescue'] = DEFAULT_NULL_DATETIME

                if has_count:
                    event['sync']['count'] = int(event['body']['sync']['total'])
                else:
                    event['sync']['count'] = DEFAULT_NULL_INTEGER
            else:
                event['sync'] = {
                    'first_controller': DEFAULT_NULL_DATETIME,
                    'count': DEFAULT_NULL_INTEGER,
                    'has_last': DEFAULT_NULL_BOOLEAN,
                    'first_rescue': DEFAULT_NULL_DATETIME,
                    'last_controller': DEFAULT_NULL_DATETIME,
                    'has_last_rescue': DEFAULT_NULL_BOOLEAN,
                    'last_rescue': DEFAULT_NULL_DATETIME,
                    'last': DEFAULT_NULL_DATETIME,
                    'has_first_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first_rescue': DEFAULT_NULL_BOOLEAN,
                    'has_count': DEFAULT_NULL_BOOLEAN,
                    'has_last_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first': DEFAULT_NULL_BOOLEAN,
                    'first': DEFAULT_NULL_DATETIME
                }

            has_is_fake_birth_date = 'isFakeBirthDate' in event['body'] and \
                                     event['body']['isFakeBirthDate'] is not None
            event['has_is_fake_birth_date'] = has_is_fake_birth_date
            if has_is_fake_birth_date:
                event['is_fake_birth_date'] = event['body']['isFakeBirthDate']
            else:
                event['is_fake_birth_date'] = DEFAULT_NULL_BOOLEAN

            has_is_fake_name = 'isFakeName' in event['body'] and \
                               event['body']['isFakeName'] is not None
            event['has_is_fake_name'] = has_is_fake_name
            if has_is_fake_name:
                event['is_fake_name'] = event['body']['isFakeName']
            else:
                event['is_fake_name'] = DEFAULT_NULL_BOOLEAN

            has_is_phone_mobile = 'isPhoneMobile' in event['body'] and \
                                  event['body']['isPhoneMobile'] is not None
            event['has_is_phone_mobile'] = has_is_phone_mobile
            if has_is_phone_mobile:
                event['is_phone_mobile'] = event['body']['isPhoneMobile']
            else:
                event['is_phone_mobile'] = DEFAULT_NULL_BOOLEAN

            has_active_sponsorship_quotes = 'activeSponsorshipQuotes' in event['body'] and \
                                            event['body']['activeSponsorshipQuotes'] is not None and \
                                            len(event['body']['activeSponsorshipQuotes']) > 0
            event['has_active_sponsorship_quotes'] = has_active_sponsorship_quotes
            if has_active_sponsorship_quotes:
                event['active_sponsorship_quotes'] = []
                for s in event['body']['activeSponsorshipQuotes']:
                    event['active_sponsorship_quotes'].append({
                        'id': s['quoteId'],
                        'name': s['sponsorName']
                    })
            else:
                event['active_sponsorship_quotes'] = [{
                    'id': DEFAULT_NULL_STRING,
                    'name': DEFAULT_NULL_STRING
                }]

            has_triggers = 'triggers' in event['body'] and \
                           event['body']['triggers'] is not None and \
                           len(event['body']['triggers']) > 0
            event['has_triggers'] = has_triggers
            if has_triggers:
                event['triggers'] = event['body']['triggers']
            else:
                event['triggers'] = [DEFAULT_NULL_STRING]

            has_unique_external_id = 'uniqueExternalId' in event['body'] and \
                                     event['body']['uniqueExternalId'] is not None
            event['has_unique_external_id'] = has_unique_external_id
            if has_unique_external_id:
                event['unique_external_id'] = event['body']['uniqueExternalId']
            else:
                event['unique_external_id'] = DEFAULT_NULL_STRING

            if event['pubSubEvent'][:15] == "patient_created":
                has_role_transistioned_from = 'prospect' in event['pubSubArgs']
                if has_role_transistioned_from:
                    event['prospect_conversion_datetime'] = event['call_time']
                else:
                    event['prospect_conversion_datetime'] = DEFAULT_NULL_DATETIME

            if 'hispanic' in event['body']:
                hispanic = event['body']['hispanic']
                if hispanic == 'null':
                    event['hispanic'] = 'no answer'
                else:
                    event['hispanic'] = hispanic
            else:
                event['hispanic'] = None


            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_patient', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            print('error in map_patient', file=sys.stderr)
            print(type(e), file=sys.stderr)
            print(e.args, file=sys.stderr)
            print(e, file=sys.stderr)
            print(event, file=sys.stderr)
            raise

    def map_medication(self, event):
        try:
            pub_sub_args = get_pubSubArgs(event)
            mid_changed = pub_sub_args[0]
            event = self.map_patient(event)
            event['medication_changed'] = None
            event['medication_changed_date'] = DEFAULT_NULL_DATETIME
            if mid_changed.startswith("{"):
                mid_changed = ast.literal_eval(mid_changed)['mid']
            event['medication_changed'] = mid_changed
            event['medication_changed_date'] = event['call_time']
            return event
        except Exception as e:
            print('error in map_medication', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            print('error in map_medication', file=sys.stderr)
            print(type(e), file=sys.stderr)
            print(e.args, file=sys.stderr)
            print(e, file=sys.stderr)
            print(event, file=sys.stderr)
            raise

    def map_patient_login(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_patient_login', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_prospect(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            has_role = 'role' in event['body'] and \
                       event['body']['role'] is not None
            if not has_role:
                print(
                    'non prospect role sent to map_prospect!',
                    file=sys.stdout
                )
                return event

            # there are 2 different major versions in play here
            # API 3.x & API 4.x. They are similar enough that we
            # just if'd it out. However, it might be wise to
            # somehow abstract this better. The switch is datalake
            # pubsub 1.7.0 version where we first started sending
            # API 4.x user data structures

            call_time = try_parsing_date(event['callTime'])
            date_created = try_parsing_date(event['body']['createdDate'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['role'] = event['body']['role']
            event['date_created'] = datetime_to_iso_str(date_created)
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            # Has to have at least one of the following for email, phone, unique external id, or oidcidtoken
            has_email = 'email' in event['body'] \
                        and event['body']['email'] is not None
            event['has_email'] = has_email
            if has_email:
                event['email'] = str(event['body']['email'])
            else:
                event['email'] = DEFAULT_NULL_STRING

            has_phone = 'phone' in event['body'] \
                        and event['body']['phone'] is not None
            event['has_phone'] = has_phone

            if has_phone:
                event['phone'] = str(event['body']['phone'])
            else:
                event['phone'] = DEFAULT_NULL_STRING

            has_unique_external_id = 'uniqueExternalId' in event['body'] and \
                                     event['body']['uniqueExternalId'] is not None
            event['has_unique_external_id'] = has_unique_external_id
            if has_unique_external_id:
                event['unique_external_id'] = event['body']['uniqueExternalId']
            else:
                event['unique_external_id'] = DEFAULT_NULL_STRING

            has_oidcidtoken = 'oidcidtoken' in event['body'] \
                              and event['body']['oidcidtoken'] is not None
            event['has_oidcidtoken'] = has_oidcidtoken
            if has_oidcidtoken:
                event['oidcidtoken'] = str(event['body']['oidcidtoken'])
            else:
                event['oidcidtoken'] = DEFAULT_NULL_STRING

            has_group = 'group' in event['body'] \
                        and event['body']['group'] is not None

            event['has_group'] = has_group
            if has_group:
                event['group'] = {
                    'name': event['body']['group']['name'],
                    'display_name': event['body']['group']['displayName']
                }
            else:
                event['group'] = {
                    'name': DEFAULT_NULL_STRING,
                    'display_name': DEFAULT_NULL_STRING
                }

            has_given_name = 'givenName' in event['body'] \
                             and event['body']['givenName'] is not None

            event['has_given_name'] = has_given_name
            if has_given_name:
                event['given_name'] = event['body']['givenName']
            else:
                event['given_name'] = DEFAULT_NULL_STRING

            has_time_zone = 'timeZone' in event['body'] \
                            and event['body']['timeZone'] is not None

            event['has_time_zone'] = has_time_zone
            if has_time_zone:
                event['time_zone'] = event['body']['timeZone']
            else:
                event['time_zone'] = DEFAULT_NULL_STRING

            has_family_name = 'familyName' in event['body'] \
                              and event['body']['familyName'] is not None

            event['has_family_name'] = has_family_name
            if has_family_name:
                event['family_name'] = event['body']['familyName']
            else:
                event['family_name'] = DEFAULT_NULL_STRING

            has_language = 'language' in event['body'] \
                           and event['body']['language'] is not None

            event['has_language'] = has_language
            if has_language:
                event['language'] = event['body']['language']
            else:
                event['language'] = DEFAULT_NULL_STRING

            has_age = 'age' in event['body'] \
                      and event['body']['age'] is not None

            event['has_age'] = has_age
            if has_age:
                event['age'] = event['body']['age']
            else:
                event['age'] = DEFAULT_NULL_INTEGER

            has_birth_date = 'birthDate' in event['body'] \
                             and event['body']['birthDate'] is not None

            event['has_birth_date'] = has_birth_date
            if has_birth_date:
                event['birth_date'] = event['body']['birthDate']
            else:
                event['birth_date'] = DEFAULT_NULL_STRING

            has_disease = 'disease' in event['body'] \
                          and event['body']['disease'] is not None

            event['has_disease'] = has_disease
            if has_disease:
                event['disease'] = event['body']['disease']
            else:
                event['disease'] = DEFAULT_NULL_STRING

            # followers
            has_followers = 'followers' in event['body'] \
                            and event['body']['followers'] is not None \
                            and len(event['body']['followers']) > 0
            if has_followers:
                event['followers'] = []
                for f in event['body']['followers']:
                    f2 = {}
                    has_user_id = 'id' in f \
                                  and f['id'] is not None
                    f2['has_user_id'] = has_user_id
                    if has_user_id:
                        f2['user_id'] = f['id']
                    else:
                        f2['user_id'] = DEFAULT_NULL_STRING

                    has_access_level = 'accessLevel' in f \
                                       and f['accessLevel'] is not None
                    f2['has_access_level'] = has_access_level
                    if has_access_level:
                        f2['access_level'] = f['accessLevel']
                    else:
                        f2['access_level'] = DEFAULT_NULL_STRING

                    has_given_name = 'givenName' in f \
                                     and f['givenName'] is not None
                    f2['has_given_name'] = has_given_name
                    if has_given_name:
                        f2['given_name'] = f['givenName']
                    else:
                        f2['given_name'] = DEFAULT_NULL_STRING

                    has_family_name = 'familyName' in f \
                                      and f['familyName'] is not None
                    f2['has_family_name'] = has_family_name
                    if has_family_name:
                        f2['family_name'] = f['familyName']
                    else:
                        f2['family_name'] = DEFAULT_NULL_STRING

                    has_role = 'role' in f \
                               and f['role'] is not None
                    f2['has_role'] = has_role
                    if has_role:
                        f2['role'] = f['role']
                    else:
                        f2['role'] = DEFAULT_NULL_STRING

                    event['followers'].append(f2)
            else:
                event['followers'] = [{
                    'role': DEFAULT_NULL_STRING,
                    'access_level': DEFAULT_NULL_STRING,
                    'has_access_level': DEFAULT_NULL_BOOLEAN,
                    'has_user_id': DEFAULT_NULL_BOOLEAN,
                    'has_family_name': DEFAULT_NULL_BOOLEAN,
                    'has_role': DEFAULT_NULL_BOOLEAN,
                    'has_given_name': DEFAULT_NULL_BOOLEAN,
                    'given_name': DEFAULT_NULL_STRING,
                    'family_name': DEFAULT_NULL_STRING,
                    'user_id': DEFAULT_NULL_STRING
                }]


            # following
            has_following = 'following' in event['body'] \
                            and event['body']['following'] is not None \
                            and len(event['body']['following']) > 0
            if has_following:
                event['following'] = []
                for f in event['body']['following']:
                    f2 = {}
                    has_user_id = 'id' in f \
                                  and f['id'] is not None
                    f2['has_user_id'] = has_user_id
                    if has_user_id:
                        f2['user_id'] = f['id']
                    else:
                        f2['user_id'] = DEFAULT_NULL_STRING

                    has_access_level = 'accessLevel' in f \
                                       and f['accessLevel'] is not None
                    f2['has_access_level'] = has_access_level
                    if has_access_level:
                        f2['access_level'] = f['accessLevel']
                    else:
                        f2['access_level'] = DEFAULT_NULL_STRING

                    has_given_name = 'givenName' in f \
                                     and f['givenName'] is not None
                    f2['has_given_name'] = has_given_name
                    if has_given_name:
                        f2['given_name'] = f['givenName']
                    else:
                        f2['given_name'] = DEFAULT_NULL_STRING

                    has_family_name = 'familyName' in f \
                                      and f['familyName'] is not None
                    f2['has_family_name'] = has_family_name
                    if has_family_name:
                        f2['family_name'] = f['familyName']
                    else:
                        f2['family_name'] = DEFAULT_NULL_STRING

                    has_role = 'role' in f \
                               and f['role'] is not None
                    f2['has_role'] = has_role
                    if has_role:
                        f2['role'] = f['role']
                    else:
                        f2['role'] = DEFAULT_NULL_STRING

                    has_disease = 'disease' in f \
                                  and f['disease'] is not None
                    f2['has_disease'] = has_disease
                    if has_disease:
                        f2['disease'] = f['disease']
                    else:
                        f2['disease'] = DEFAULT_NULL_STRING

                    event['following'].append(f2)
            else:
                event['following'] = [{
                    'role': DEFAULT_NULL_STRING,
                    'access_level': DEFAULT_NULL_STRING,
                    'has_access_level': DEFAULT_NULL_BOOLEAN,
                    'has_disease': DEFAULT_NULL_BOOLEAN,
                    'has_family_name': DEFAULT_NULL_BOOLEAN,
                    'has_user_id': DEFAULT_NULL_BOOLEAN,
                    'has_role': DEFAULT_NULL_BOOLEAN,
                    'has_given_name': DEFAULT_NULL_BOOLEAN,
                    'disease': DEFAULT_NULL_STRING,
                    'given_name': DEFAULT_NULL_STRING,
                    'family_name': DEFAULT_NULL_STRING,
                    'user_id': DEFAULT_NULL_STRING
                }]

            # optional notifications
            has_notifications = 'notifications' in event['body'] \
                                and event['body']['notifications'] is not None
            if has_notifications:
                event['notifications'] = {}
                has_digest = 'digest' in event['body']['notifications'] \
                             and event['body']['notifications']['digest'] is not None
                event['notifications']['has_digest'] = has_digest
                if has_digest:
                    event['notifications']['digest_email'] = event['body']['notifications']['digest']['email']
                else:
                    event['notifications']['digest_email'] = DEFAULT_NULL_BOOLEAN

                has_notes = 'notes' in event['body']['notifications'] \
                            and event['body']['notifications']['notes'] is not None
                event['notifications']['has_notes'] = has_notes
                if has_notes:
                    event['notifications']['notes_email'] = event['body']['notifications']['notes']['email']
                    event['notifications']['notes_sms'] = event['body']['notifications']['notes']['sms']
                    event['notifications']['notes_push'] = event['body']['notifications']['notes']['push']
                else:
                    event['notifications']['notes_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['notes_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['notes_push'] = DEFAULT_NULL_BOOLEAN

                has_missed_dose = 'missedDose' in event['body']['notifications'] \
                                  and event['body']['notifications']['missedDose'] is not None
                event['body']['notifications']['has_missed_dose'] = has_missed_dose
                if has_missed_dose:
                    event['notifications']['missed_dose_email'] = event['body']['notifications']['missedDose']['email']
                    event['notifications']['missed_dose_sms'] = event['body']['notifications']['missedDose']['sms']
                    event['notifications']['missed_dose_push'] = event['body']['notifications']['missedDose']['push']
                else:
                    event['notifications']['missed_dose_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['missed_dose_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['missed_dose_push'] = DEFAULT_NULL_BOOLEAN

                has_rescue_usage = 'rescueUsage' in event['body']['notifications'] \
                                   and event['body']['notifications']['rescueUsage'] is not None
                event['notifications']['has_rescue_usage'] = has_rescue_usage
                if has_rescue_usage:
                    event['notifications']['rescue_usage_email'] = event['body']['notifications']['rescueUsage']['email']
                    event['notifications']['rescue_usage_sms'] = event['body']['notifications']['rescueUsage']['sms']
                    event['notifications']['rescue_usage_push'] = event['body']['notifications']['rescueUsage']['push']
                else:
                    event['notifications']['rescue_usage_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['rescue_usage_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['rescue_usage_push'] = DEFAULT_NULL_BOOLEAN

                has_goal = 'goal' in event['body']['notifications'] \
                           and event['body']['notifications']['goal'] is not None
                event['notifications']['has_goal'] = has_goal
                if has_goal:
                    event['notifications']['goal_email'] = event['body']['notifications']['goal']['email']
                    event['notifications']['goal_sms'] = event['body']['notifications']['goal']['sms']
                    event['notifications']['goal_push'] = event['body']['notifications']['goal']['push']
                else:
                    event['notifications']['goal_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['goal_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['goal_push'] = DEFAULT_NULL_BOOLEAN

                has_quiet_sensor = 'quietSensor' in event['body']['notifications'] \
                                   and event['body']['notifications']['quietSensor'] is not None
                event['notifications']['has_quiet_sensor'] = has_quiet_sensor
                if has_quiet_sensor:
                    event['notifications']['quiet_sensor_email'] = event['body']['notifications']['quietSensor']['email']
                    event['notifications']['quiet_sensor_sms'] = event['body']['notifications']['quietSensor']['sms']
                    event['notifications']['quiet_sensor_push'] = event['body']['notifications']['quietSensor']['push']
                else:
                    event['notifications']['quiet_sensor_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['quiet_sensor_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['quiet_sensor_push'] = DEFAULT_NULL_BOOLEAN

                has_act_invite = 'actInvite' in event['body']['notifications'] \
                                 and event['body']['notifications']['actInvite'] is not None
                event['notifications']['has_act_invite'] = has_act_invite
                if has_act_invite:
                    event['notifications']['act_invite_email'] = event['body']['notifications']['actInvite']['email']
                    event['notifications']['act_invite_sms'] = event['body']['notifications']['actInvite']['sms']
                    event['notifications']['act_invite_push'] = event['body']['notifications']['actInvite']['push']
                else:
                    # API 4.x renamed to questionnaire
                    has_act_invite = 'questionnaire' in event['body']['notifications'] \
                                     and event['body']['notifications']['questionnaire'] is not None
                    event['notifications']['has_act_invite'] = has_act_invite
                    if has_act_invite:
                        event['notifications']['act_invite_email'] = event['body']['notifications']['questionnaire']['email']
                        event['notifications']['act_invite_sms'] = event['body']['notifications']['questionnaire']['sms']
                        event['notifications']['act_invite_push'] = event['body']['notifications']['questionnaire']['push']
                    else:
                        event['notifications']['act_invite_email'] = DEFAULT_NULL_BOOLEAN
                        event['notifications']['act_invite_sms'] = DEFAULT_NULL_BOOLEAN
                        event['notifications']['act_invite_push'] = DEFAULT_NULL_BOOLEAN

                has_transition = 'transition' in event['body']['notifications'] \
                                 and event['body']['notifications']['transition'] is not None
                event['notifications']['has_transition'] = has_transition
                if has_transition:
                    event['notifications']['transition_email'] = event['body']['notifications']['transition']['email']
                    event['notifications']['transition_sms'] = event['body']['notifications']['transition']['sms']
                    event['notifications']['transition_push'] = event['body']['notifications']['transition']['push']
                else:
                    event['notifications']['transition_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['transition_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['transition_push'] = DEFAULT_NULL_BOOLEAN

                has_environmental = 'environmental' in event['body']['notifications'] \
                                    and event['body']['notifications']['environmental'] is not None
                event['notifications']['has_environmental'] = has_environmental
                if has_environmental:
                    event['notifications']['environmental_email'] = event['body']['notifications']['environmental']['email']
                    event['notifications']['environmental_sms'] = event['body']['notifications']['environmental']['sms']
                    event['notifications']['environmental_push'] = event['body']['notifications']['environmental']['push']
                else:
                    event['notifications']['environmental_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['environmental_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['environmental_push'] = DEFAULT_NULL_BOOLEAN

                has_at_risk = 'atRisk' in event['body']['notifications'] \
                              and event['body']['notifications']['atRisk'] is not None
                event['notifications']['has_at_risk'] = has_at_risk
                if has_at_risk:
                    event['notifications']['at_risk_email'] = event['body']['notifications']['atRisk']['email']
                    event['notifications']['at_risk_sms'] = event['body']['notifications']['atRisk']['sms']
                    event['notifications']['at_risk_push'] = event['body']['notifications']['atRisk']['push']
                else:
                    event['notifications']['at_risk_email'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['at_risk_sms'] = DEFAULT_NULL_BOOLEAN
                    event['notifications']['at_risk_push'] = DEFAULT_NULL_BOOLEAN
            else:
                event['notifications']  = {
                    'transition_sms': DEFAULT_NULL_BOOLEAN,
                    'transition_email': DEFAULT_NULL_BOOLEAN,
                    'goal_email': DEFAULT_NULL_BOOLEAN,
                    'has_goal': DEFAULT_NULL_BOOLEAN,
                    'notes_push': DEFAULT_NULL_BOOLEAN,
                    'quiet_sensor_push': DEFAULT_NULL_BOOLEAN,
                    'goal_sms': DEFAULT_NULL_BOOLEAN,
                    'goal_push': DEFAULT_NULL_BOOLEAN,
                    'has_rescue_usage': DEFAULT_NULL_BOOLEAN,
                    'at_risk_email': DEFAULT_NULL_BOOLEAN,
                    'quiet_sensor_sms': DEFAULT_NULL_BOOLEAN,
                    'missed_dose_email': DEFAULT_NULL_BOOLEAN,
                    'at_risk_push': DEFAULT_NULL_BOOLEAN,
                    'has_notes': DEFAULT_NULL_BOOLEAN,
                    'quiet_sensor_email': DEFAULT_NULL_BOOLEAN,
                    'transition_push': DEFAULT_NULL_BOOLEAN,
                    'at_risk_sms': DEFAULT_NULL_BOOLEAN,
                    'rescue_usage_email': DEFAULT_NULL_BOOLEAN,
                    'environmental_push': DEFAULT_NULL_BOOLEAN,
                    'missed_dose_sms': DEFAULT_NULL_BOOLEAN,
                    'act_invite_sms': DEFAULT_NULL_BOOLEAN,
                    'act_invite_email': DEFAULT_NULL_BOOLEAN,
                    'notes_sms': DEFAULT_NULL_BOOLEAN,
                    'act_invite_push': DEFAULT_NULL_BOOLEAN,
                    'missed_dose_push': DEFAULT_NULL_BOOLEAN,
                    'rescue_usage_sms': DEFAULT_NULL_BOOLEAN,
                    'rescue_usage_push': DEFAULT_NULL_BOOLEAN,
                    'environmental_email': DEFAULT_NULL_BOOLEAN,
                    'digest_email': DEFAULT_NULL_BOOLEAN,
                    'has_transition': DEFAULT_NULL_BOOLEAN,
                    'has_act_invite': DEFAULT_NULL_BOOLEAN,
                    'notes_email': DEFAULT_NULL_BOOLEAN,
                    'has_environmental': DEFAULT_NULL_BOOLEAN,
                    'has_quiet_sensor': DEFAULT_NULL_BOOLEAN,
                    'has_at_risk': DEFAULT_NULL_BOOLEAN,
                    'environmental_sms': DEFAULT_NULL_BOOLEAN,
                    'has_digest': DEFAULT_NULL_BOOLEAN
                }
            # medications
            has_medications = 'plan' in event['body'] \
                              and event['body']['plan'] is not None \
                              and 'medications' in event['body']['plan'] \
                              and event['body']['plan']['medications'] is not None \
                              and len(event['body']['plan']['medications']) > 0
            event['has_medications'] = has_medications
            event['usage_list'] = []
            event['sensors'] = []
            if has_medications:
                event['medications'] = []
                for m in event['body']['plan']['medications']:
                    medication = {
                        'medication_id': m['medication']['id']
                    }
                    has_data_link = 'dataLink' in m \
                                    and m['dataLink'] is not None
                    medication['has_data_link'] = has_data_link
                    if has_data_link:
                        medication['data_link'] = m['dataLink']
                    else:
                        medication['data_link'] = DEFAULT_NULL_BOOLEAN

                    # API 4.x periods
                    has_effective_period = 'effectivePeriod' in m \
                                           and m['effectivePeriod'] is not None
                    medication['has_effective_period_start'] = has_effective_period
                    if has_effective_period:
                        medication['effective_period_start'] = m['effectivePeriod']['start']
                        has_effective_period_end = 'end' in m['effectivePeriod'] and \
                                                   m['effectivePeriod']['end'] is not None
                        medication['has_effective_period_end'] = has_effective_period
                        if has_effective_period_end:
                            medication['effective_period_end'] = m['effectivePeriod']['end']
                        else:
                            medication['effective_period_end'] = DEFAULT_NULL_DATETIME
                    else:
                        medication['has_effective_period_end'] = has_effective_period
                        medication['effective_period_start'] = DEFAULT_NULL_DATETIME
                        medication['effective_period_end'] = DEFAULT_NULL_DATETIME

                    # API 4.x status
                    has_status = 'status' in m and \
                                 m['status'] is not None
                    medication['has_status'] = has_status
                    if has_status:
                        medication['status'] = m['status']
                    else:
                        medication['status'] = DEFAULT_NULL_STRING

                    event['medications'].append(medication)

                    has_usage_list = 'usageList' in m \
                                     and m['usageList'] is not None \
                                     and len(m['usageList']) > 0
                    if has_usage_list:
                        for ul in m['usageList']:
                            dose_time = {
                                'medication_id': m['medication']['id'],
                                'hour': ul['hour'],
                                'minute': ul['minute']
                            }
                            has_doses = 'doses' in ul and \
                                        ul['doses'] is not None
                            if has_doses:
                                dose_time['doses'] = ul['doses']
                            else:
                                # API 4.x updated to unitDoses
                                has_unit_doses = 'unitDoses' in ul and \
                                                 ul['unitDoses'] is not None
                                if has_unit_doses:
                                    dose_time['doses'] = ul['unitDoses']

                            event['usage_list'].append(dose_time)

                    has_sensors = 'sensors' in m \
                                  and m['sensors'] is not None \
                                  and len(m['sensors']) > 0
                    if has_sensors:
                        for s in m['sensors']:
                            sensor = {
                                'mac': s['mac'],
                                'medication_id': m['medication']['id']
                            }

                            # add medication type
                            has_medication_type = 'type' in m['medication'] \
                                                  and m['medication']['type'] is not None
                            sensor['has_medication_type'] = has_medication_type
                            if has_medication_type:
                                sensor['medication_type'] = m['medication']['type']
                            else:
                                sensor['medication_type'] = DEFAULT_NULL_STRING

                            # optional sensor fields
                            has_battery = 'battery' in s \
                                          and s['battery'] is not None
                            sensor['has_battery'] = has_battery
                            if has_battery:
                                sensor['battery'] = s['battery']
                            else:
                                # battery renamed to batteryLevel in 4.x
                                has_battery = 'batteryLevel' in s \
                                              and s['batteryLevel'] is not None
                                sensor['has_battery'] = has_battery
                                if has_battery:
                                    sensor['battery'] = s['batteryLevel']
                                else:
                                    sensor['battery'] = DEFAULT_NULL_INTEGER

                            has_data_link = 'dataLink' in s \
                                            and s['dataLink'] is not None
                            sensor['has_data_link'] = has_data_link
                            if has_data_link:
                                sensor['data_link'] = s['dataLink']
                            else:
                                sensor['data_link'] = DEFAULT_NULL_BOOLEAN

                            has_first_sync_date = 'firstSyncDate' in s \
                                                  and s['firstSyncDate'] is not None
                            sensor['has_first_sync_date'] = has_first_sync_date
                            if has_first_sync_date:
                                sensor['first_sync_date'] = s['firstSyncDate']
                            else:
                                sensor['first_sync_date'] = DEFAULT_NULL_DATETIME

                            has_firmware_version = 'firmwareVersion' in s \
                                                   and s['firmwareVersion'] is not None
                            sensor['has_firmware_version'] = has_firmware_version
                            if has_firmware_version:
                                sensor['firmware_version'] = s['firmwareVersion']
                            else:
                                sensor['firmware_version'] = DEFAULT_NULL_STRING

                            has_force_silent = 'forceSilent' in s \
                                               and s['forceSilent'] is not None
                            sensor['has_force_silent'] = has_force_silent
                            if has_force_silent:
                                sensor['force_silent'] = s['forceSilent']
                            else:
                                sensor['force_silent'] = DEFAULT_NULL_BOOLEAN

                            has_last_sync_date = 'lastSyncDate' in s \
                                                 and s['lastSyncDate'] is not None
                            sensor['has_last_sync_date'] = has_last_sync_date
                            if has_last_sync_date:
                                sensor['last_sync_date'] = s['lastSyncDate']
                            else:
                                sensor['last_sync_date'] = DEFAULT_NULL_DATETIME

                            has_model = 'model' in s and s['model'] is not None
                            sensor['has_model'] = has_model
                            if has_model:
                                sensor['model'] = s['model']
                            else:
                                sensor['model'] = DEFAULT_NULL_STRING

                            has_ready_date = 'readyDate' in s \
                                             and s['readyDate'] is not None
                            sensor['has_ready_date'] = has_ready_date
                            if has_ready_date:
                                sensor['ready_date'] = s['readyDate']
                            else:
                                sensor['ready_date'] = DEFAULT_NULL_DATETIME

                            has_silent = 'silent' in s \
                                         and s['silent'] is not None
                            sensor['has_silent'] = has_silent
                            if has_silent:
                                sensor['silent'] = s['silent']
                            else:
                                sensor['silent'] = DEFAULT_NULL_BOOLEAN

                            event['sensors'].append(sensor)
            else:
                event['medications'] = [{
                    'has_status': DEFAULT_NULL_BOOLEAN,
                    'has_effective_period_end': DEFAULT_NULL_BOOLEAN,
                    'has_effective_period_start': DEFAULT_NULL_BOOLEAN,
                    'effective_period_end': DEFAULT_NULL_DATETIME,
                    'data_link': DEFAULT_NULL_BOOLEAN,
                    'has_data_link': DEFAULT_NULL_BOOLEAN,
                    'status': DEFAULT_NULL_STRING,
                    'medication_id': DEFAULT_NULL_STRING,
                    'effective_period_start': DEFAULT_NULL_DATETIME
                }]
            has_usage_list = len(event['usage_list']) > 0
            has_sensors = len(event['sensors']) > 0
            event['has_usage_list'] = has_usage_list
            event['has_sensors'] = has_sensors

            if not has_usage_list:
                event['usage_list'].append({
                    'doses': DEFAULT_NULL_INTEGER,
                    'hour': DEFAULT_NULL_INTEGER,
                    'medication_id': DEFAULT_NULL_STRING,
                    'minute': DEFAULT_NULL_INTEGER
                })

            if not has_sensors:
                event['sensors'].append({
                    'has_silent': DEFAULT_NULL_BOOLEAN,
                    'has_model': DEFAULT_NULL_BOOLEAN,
                    'medication_type': DEFAULT_NULL_STRING,
                    'force_silent': DEFAULT_NULL_BOOLEAN,
                    'has_ready_date': DEFAULT_NULL_BOOLEAN,
                    'mac': DEFAULT_NULL_STRING,
                    'has_firmware_version': DEFAULT_NULL_BOOLEAN,
                    'model': DEFAULT_NULL_STRING,
                    'ready_date': DEFAULT_NULL_DATETIME,
                    'last_sync_date': DEFAULT_NULL_DATETIME,
                    'has_force_silent': DEFAULT_NULL_BOOLEAN,
                    'has_first_sync_date': DEFAULT_NULL_BOOLEAN,
                    'silent': DEFAULT_NULL_BOOLEAN,
                    'has_medication_type': DEFAULT_NULL_BOOLEAN,
                    'data_link': DEFAULT_NULL_BOOLEAN,
                    'has_battery': DEFAULT_NULL_BOOLEAN,
                    'has_data_link': DEFAULT_NULL_BOOLEAN,
                    'medication_id': DEFAULT_NULL_STRING,
                    'battery': DEFAULT_NULL_INTEGER,
                    'first_sync_date': DEFAULT_NULL_DATETIME,
                    'has_last_sync_date': DEFAULT_NULL_BOOLEAN,
                    'firmware_version': DEFAULT_NULL_STRING
                })

            # mailing_address
            has_mailing_address = 'mailingAddress' in event['body'] \
                                  and event['body']['mailingAddress'] is not None
            if has_mailing_address:
                event['mailing_address'] = {
                    'street': event['body']['mailingAddress']['street'],
                    'city': event['body']['mailingAddress']['city'],
                    'country': event['body']['mailingAddress']['country'],
                }

                # check & add the optional fields for mailingAddresses globally
                has_postal_code = 'postalCode' in event['body']['mailingAddress'] \
                                  and event['body']['mailingAddress']['postalCode'] is not None
                event['mailing_address']['has_postal_code'] = has_postal_code
                if has_postal_code:
                    event['mailing_address']['postal_code'] = event['body']['mailingAddress']['postalCode']
                else:
                    event['mailing_address']['postal_code'] = DEFAULT_NULL_STRING

                has_state_or_region = 'stateOrRegion' in event['body']['mailingAddress'] \
                                      and event['body']['mailingAddress']['stateOrRegion'] is not None
                event['mailing_address']['has_state_or_region'] = has_state_or_region
                if has_state_or_region:
                    event['mailing_address']['state_or_region'] = event['body']['mailingAddress']['stateOrRegion']
                else:
                    event['mailing_address']['state_or_region'] = DEFAULT_NULL_STRING

                has_street2 = 'street2' in event['body']['mailingAddress'] \
                              and event['body']['mailingAddress']['street2'] is not None
                event['mailing_address']['has_street2'] = has_street2
                if has_street2:
                    event['mailing_address']['street2'] = event['body']['mailingAddress']['street2']
                else:
                    event['mailing_address']['street2'] = DEFAULT_NULL_STRING

                has_state_code = 'stateCode' in event['body']['mailingAddress'] \
                                 and event['body']['mailingAddress']['stateCode'] is not None
                event['mailing_address']['has_state_code'] = has_state_code
                if has_state_code:
                    event['mailing_address']['state_code'] = event['body']['mailingAddress']['stateCode']
                else:
                    event['mailing_address']['state_code'] = DEFAULT_NULL_STRING
            else:
                event['mailing_address'] = {
                    'state_code': DEFAULT_NULL_STRING,
                    'has_street2': DEFAULT_NULL_BOOLEAN,
                    'city': DEFAULT_NULL_STRING,
                    'country': DEFAULT_NULL_STRING,
                    'street2': DEFAULT_NULL_STRING,
                    'has_state_or_region': DEFAULT_NULL_BOOLEAN,
                    'has_postal_code': DEFAULT_NULL_BOOLEAN,
                    'postal_code': DEFAULT_NULL_STRING,
                    'has_state_code': DEFAULT_NULL_BOOLEAN,
                    'street': DEFAULT_NULL_STRING,
                    'state_or_region': DEFAULT_NULL_STRING
                }

            # gender
            has_gender = 'gender' in event['body'] \
                         and event['body']['gender'] is not None
            event['has_gender'] = has_gender
            if has_gender:
                event['gender'] = event['body']['gender']
            else:
                event['gender'] = DEFAULT_NULL_STRING

            # height
            has_height = 'height' in event['body'] \
                         and event['body']['height'] is not None
            event['has_height'] = has_height
            if has_height:
                if isinstance(event['body']['height'], dict):
                    # API 4.x is a struct
                    event['height'] = float(event['body']['height']['value'])
                else:
                    event['height'] = float(event['body']['height'])
            else:
                event['height'] = DEFAULT_NULL_DOUBLE

            # medical_ids
            has_medical_ids = 'medicalIds' in event['body'] \
                              and event['body']['medicalIds'] is not None \
                              and len(event['body']['medicalIds']) > 0
            event['has_medical_ids'] = has_medical_ids
            if has_medical_ids:
                event['medical_ids'] = []
                for m in event['body']['medicalIds']:
                    event['medical_ids'].append({
                        'key': m['key'],
                        'value': m['value']
                    })
            else:
                # API 4.x renamed
                has_medical_ids = 'externalIds' in event['body'] \
                                  and event['body']['externalIds'] is not None \
                                  and len(event['body']['externalIds']) > 0
                event['has_medical_ids'] = has_medical_ids
                if has_medical_ids:
                    event['medical_ids'] = []
                    for m in event['body']['externalIds']:
                        event['medical_ids'].append({
                            'key': m['key'],
                            'value': m['value']
                        })
                else:
                    event['medical_ids'] = [{
                        'key': DEFAULT_NULL_STRING,
                        'value': DEFAULT_NULL_STRING
                    }]

            # hub
            has_hub = 'hub' in event['body'] \
                      and event['body']['hub'] is not None
            event['has_hub'] = has_hub
            if has_hub:
                event['hub'] = {}
                has_id = 'id' in event['body']['hub'] \
                         and event['body']['hub']['id'] is not None
                event['hub']['has_id'] = has_id
                if has_id:
                    event['hub']['id'] = event['body']['hub']['id']
                else:
                    event['hub']['id'] = DEFAULT_NULL_STRING

                has_external_id = 'externalId' in event['body']['hub'] \
                                  and event['body']['hub']['externalId'] is not None
                event['hub']['has_external_id'] = has_external_id
                if has_external_id:
                    event['hub']['external_id'] = event['body']['hub']['externalId']
                else:
                    event['hub']['external_id'] = DEFAULT_NULL_STRING

                has_type = 'type' in event['body']['hub'] \
                           and event['body']['hub']['type'] is not None
                event['hub']['has_type'] = has_type
                if has_type:
                    event['hub']['type'] = event['body']['hub']['type']
                else:
                    event['hub']['type'] = DEFAULT_NULL_STRING

                has_region = 'region' in event['body']['hub'] \
                             and event['body']['hub']['region'] is not None
                event['hub']['has_region'] = has_region
                if has_region:
                    event['hub']['region'] = event['body']['hub']['region']
                else:
                    event['hub']['region'] = DEFAULT_NULL_STRING
            else:
                event['hub'] = {
                    'type': DEFAULT_NULL_STRING,
                    'has_id': DEFAULT_NULL_BOOLEAN,
                    'region': DEFAULT_NULL_STRING,
                    'has_region': DEFAULT_NULL_BOOLEAN,
                    'has_external_id': DEFAULT_NULL_BOOLEAN,
                    'external_id': DEFAULT_NULL_STRING,
                    'id': DEFAULT_NULL_STRING,
                    'has_type': DEFAULT_NULL_BOOLEAN
                }

            # weight
            has_weight = 'weight' in event['body'] \
                         and event['body']['weight'] is not None
            event['has_weight'] = has_weight
            if has_weight:
                if isinstance(event['body']['weight'], dict):
                    # API 4.x is a struct
                    event['weight'] = float(event['body']['weight']['value'])
                else:
                    event['weight'] = float(event['body']['weight'])
            else:
                event['weight'] = DEFAULT_NULL_DOUBLE

            # race
            has_race = 'race' in event['body'] \
                       and event['body']['race'] is not None
            event['has_race'] = has_race
            if has_race:
                event['race'] = event['body']['race']
            else:
                event['race'] = DEFAULT_NULL_STRING

            # clinical trial
            has_clinical_trial = 'clinicalTrial' in event['body'] \
                                 and event['body']['clinicalTrial'] is not None
            event['has_clinical_trial'] = has_clinical_trial
            if has_clinical_trial:
                event['clinical_trial'] = {}
                has_clinical_trial_id = 'clinicalTrialId' in event['body']['clinicalTrial'] \
                                        and event['body']['clinicalTrial']['clinicalTrialId'] is not None
                event['clinical_trial']['has_id'] = has_clinical_trial_id
                if has_clinical_trial_id:
                    event['clinical_trial']['id'] = event['body']['clinicalTrial']['clinicalTrialId']
                else:
                    event['clinical_trial']['id'] = DEFAULT_NULL_STRING

                has_arm_id = 'clinicalTrialArmId' in event['body']['clinicalTrial'] \
                             and event['body']['clinicalTrial']['clinicalTrialArmId'] is not None
                event['clinical_trial']['has_arm_id'] = has_arm_id
                if has_arm_id:
                    event['clinical_trial']['arm_id'] = event['body']['clinicalTrial']['clinicalTrialArmId']
                else:
                    event['clinical_trial']['arm_id'] = DEFAULT_NULL_STRING

                has_clinical_trial_status = 'status' in event['body']['clinicalTrial'] \
                                            and event['body']['clinicalTrial']['status'] is not None
                event['clinical_trial']['has_status'] = has_clinical_trial_status
                if has_clinical_trial_status:
                    event['clinical_trial']['status'] = event['body']['clinicalTrial']['status']
                else:
                    event['clinical_trial']['status'] = DEFAULT_NULL_STRING
            else:
                event['clinical_trial'] = {
                    'has_status': DEFAULT_NULL_BOOLEAN,
                    'arm_id': DEFAULT_NULL_STRING,
                    'status': DEFAULT_NULL_STRING,
                    'has_id': DEFAULT_NULL_BOOLEAN,
                    'has_arm_id': DEFAULT_NULL_BOOLEAN,
                    'id': DEFAULT_NULL_STRING
                }

            has_event = 'event' in event['body'] \
                        and event['body']['event'] is not None
            event['has_event'] = has_event
            if has_event:
                event['event'] = {}
                has_first = 'first' in event['body']['event'] \
                            and event['body']['event']['first'] is not None
                has_first_controller = 'firstController' in event['body']['event'] \
                                       and event['body']['event']['firstController'] is not None
                has_first_rescue = 'firstRescue' in event['body']['event'] \
                                   and event['body']['event']['firstRescue'] is not None
                has_last = 'last' in event['body']['event'] \
                           and event['body']['event']['last'] is not None
                has_last_controller = 'lastController' in event['body']['event'] \
                                      and event['body']['event']['lastController'] is not None
                has_last_rescue = 'lastRescue' in event['body']['event'] \
                                  and event['body']['event']['lastRescue'] is not None
                has_count_rescue = 'totalRescue' in event['body']['event'] \
                                   and event['body']['event']['totalRescue'] is not None

                # set the has_* fields
                event['event']['has_first'] = has_first
                event['event']['has_first_controller'] = has_first_controller
                event['event']['has_first_rescue'] = has_first_rescue
                event['event']['has_last'] = has_last
                event['event']['has_last_controller'] = has_last_controller
                event['event']['has_last_rescue'] = has_last_rescue
                event['event']['has_count_rescue'] = has_count_rescue

                # set the various fields or default
                if has_first:
                    event['event']['first'] = event['body']['event']['first']
                else:
                    event['event']['first'] = DEFAULT_NULL_DATETIME

                if has_first_controller:
                    event['event']['first_controller'] = event['body']['event']['firstController']
                else:
                    event['event']['first_controller'] = DEFAULT_NULL_DATETIME

                if has_first_rescue:
                    event['event']['first_rescue'] = event['body']['event']['firstRescue']
                else:
                    event['event']['first_rescue'] = DEFAULT_NULL_DATETIME

                if has_last:
                    event['event']['last'] = event['body']['event']['last']
                else:
                    event['event']['last'] = DEFAULT_NULL_DATETIME

                if has_last_controller:
                    event['event']['last_controller'] = event['body']['event']['lastController']
                else:
                    event['event']['last_controller'] = DEFAULT_NULL_DATETIME

                if has_last_rescue:
                    event['event']['last_rescue'] = event['body']['event']['lastRescue']
                else:
                    event['event']['last_rescue'] = DEFAULT_NULL_DATETIME

                if has_count_rescue:
                    event['event']['count_rescue'] = int(event['body']['event']['totalRescue'])
                else:
                    event['event']['count_rescue'] = DEFAULT_NULL_INTEGER
            else:
                event['event'] = {
                    'first_controller': DEFAULT_NULL_DATETIME,
                    'has_last': DEFAULT_NULL_BOOLEAN,
                    'first_rescue': DEFAULT_NULL_DATETIME,
                    'last_controller': DEFAULT_NULL_DATETIME,
                    'has_last_rescue': DEFAULT_NULL_BOOLEAN,
                    'last_rescue': DEFAULT_NULL_DATETIME,
                    'last': DEFAULT_NULL_DATETIME,
                    'has_first_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first_rescue': DEFAULT_NULL_BOOLEAN,
                    'count_rescue': DEFAULT_NULL_INTEGER,
                    'has_last_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first': DEFAULT_NULL_BOOLEAN,
                    'has_count_rescue': DEFAULT_NULL_BOOLEAN,
                    'first': DEFAULT_NULL_DATETIME
                }

            has_sync = 'sync' in event['body'] \
                       and event['body']['sync'] is not None
            event['has_sync'] = has_sync
            if has_sync:
                event['sync'] = {}
                has_first = 'first' in event['body']['sync'] \
                            and event['body']['sync'] is not None
                has_first_controller = 'firstController' in event['body']['sync'] \
                                       and event['body']['sync']['firstController'] is not None
                has_first_rescue = 'firstRescue' in event['body']['sync'] \
                                   and event['body']['sync']['firstRescue'] is not None
                has_last = 'last' in event['body']['sync'] \
                           and event['body']['sync']['last'] is not None
                has_last_controller = 'lastController' in event['body']['sync'] \
                                      and event['body']['sync']['lastController'] is not None
                has_last_rescue = 'lastRescue' in event['body']['sync'] \
                                  and event['body']['sync']['lastRescue'] is not None
                has_count = 'sync' in event['body']['sync'] \
                            and event['body']['sync']['sync'] is not None

                event['sync']['has_first'] = has_first
                event['sync']['has_first_controller'] = has_first_controller
                event['sync']['has_first_rescue'] = has_first_rescue
                event['sync']['has_last'] = has_last
                event['sync']['has_last_controller'] = has_last_controller
                event['sync']['has_last_rescue'] = has_last_rescue
                event['sync']['has_count'] = has_count

                if has_first:
                    event['sync']['first'] = event['body']['sync']['first']
                else:
                    event['sync']['first'] = DEFAULT_NULL_DATETIME

                if has_first_controller:
                    event['sync']['first_controller'] = event['body']['sync']['firstController']
                else:
                    event['sync']['first_controller'] = DEFAULT_NULL_DATETIME

                if has_first_rescue:
                    event['sync']['first_rescue'] = event['body']['sync']['firstRescue']
                else:
                    event['sync']['first_rescue'] = DEFAULT_NULL_DATETIME

                if has_last:
                    event['sync']['last'] = event['body']['sync']['last']
                else:
                    event['sync']['last'] = DEFAULT_NULL_DATETIME

                if has_last_controller:
                    event['sync']['last_controller'] = event['body']['sync']['lastController']
                else:
                    event['sync']['last_controller'] = DEFAULT_NULL_DATETIME

                if has_last_rescue:
                    event['sync']['last_rescue'] = event['body']['sync']['lastRescue']
                else:
                    event['sync']['last_rescue'] = DEFAULT_NULL_DATETIME

                if has_count:
                    event['sync']['count'] = int(event['body']['sync']['total'])
                else:
                    event['sync']['count'] = DEFAULT_NULL_INTEGER
            else:
                event['sync'] = {
                    'first_controller': DEFAULT_NULL_DATETIME,
                    'count': DEFAULT_NULL_INTEGER,
                    'has_last': DEFAULT_NULL_BOOLEAN,
                    'first_rescue': DEFAULT_NULL_DATETIME,
                    'last_controller': DEFAULT_NULL_DATETIME,
                    'has_last_rescue': DEFAULT_NULL_BOOLEAN,
                    'last_rescue': DEFAULT_NULL_DATETIME,
                    'last': DEFAULT_NULL_DATETIME,
                    'has_first_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first_rescue': DEFAULT_NULL_BOOLEAN,
                    'has_count': DEFAULT_NULL_BOOLEAN,
                    'has_last_controller': DEFAULT_NULL_BOOLEAN,
                    'has_first': DEFAULT_NULL_BOOLEAN,
                    'first': DEFAULT_NULL_DATETIME
                }

            has_is_fake_birth_date = 'isFakeBirthDate' in event['body'] and \
                                     event['body']['isFakeBirthDate'] is not None
            event['has_is_fake_birth_date'] = has_is_fake_birth_date
            if has_is_fake_birth_date:
                event['is_fake_birth_date'] = event['body']['isFakeBirthDate']
            else:
                event['is_fake_birth_date'] = DEFAULT_NULL_BOOLEAN

            has_is_fake_name = 'isFakeName' in event['body'] and \
                               event['body']['isFakeName'] is not None
            event['has_is_fake_name'] = has_is_fake_name
            if has_is_fake_name:
                event['is_fake_name'] = event['body']['isFakeName']
            else:
                event['is_fake_name'] = DEFAULT_NULL_BOOLEAN

            has_is_phone_mobile = 'isPhoneMobile' in event['body'] and \
                                  event['body']['isPhoneMobile'] is not None
            event['has_is_phone_mobile'] = has_is_phone_mobile
            if has_is_phone_mobile:
                event['is_phone_mobile'] = event['body']['isPhoneMobile']
            else:
                event['is_phone_mobile'] = DEFAULT_NULL_BOOLEAN

            has_active_sponsorship_quotes = 'activeSponsorshipQuotes' in event['body'] and \
                                            event['body']['activeSponsorshipQuotes'] is not None and \
                                            len(event['body']['activeSponsorshipQuotes']) > 0
            event['has_active_sponsorship_quotes'] = has_active_sponsorship_quotes
            if has_active_sponsorship_quotes:
                event['active_sponsorship_quotes'] = []
                for s in event['body']['activeSponsorshipQuotes']:
                    event['active_sponsorship_quotes'].append({
                        'id': s['quoteId'],
                        'name': s['sponsorName']
                    })
            else:
                event['active_sponsorship_quotes'] = [{
                    'id': DEFAULT_NULL_STRING,
                    'name': DEFAULT_NULL_STRING
                }]

            has_triggers = 'triggers' in event['body'] and \
                           event['body']['triggers'] is not None and \
                           len(event['body']['triggers']) > 0
            event['has_triggers'] = has_triggers
            if has_triggers:
                event['triggers'] = event['body']['triggers']
            else:
                event['triggers'] = [DEFAULT_NULL_STRING]

            if 'hispanic' in event['body']:
                hispanic = event['body']['hispanic']
                if hispanic == 'null':
                    event['hispanic'] = 'no answer'
                else:
                    event['hispanic'] = hispanic
            else:
                event['hispanic'] = None

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_prospect', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            print('error in map_prospect', file=sys.stderr)
            print(type(e), file=sys.stderr)
            print(e.args, file=sys.stderr)
            print(e, file=sys.stderr)
            print(event, file=sys.stderr)
            raise

    def map_event(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            # parse the dates
            call_time = try_parsing_date(event['callTime'])
            # date = dt_with_offset_parse(event['body']['date'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            has_event_id = 'id' in event['body'] \
                           and event['body']['id'] is not None
            if has_event_id:
                event['event_id'] = event['body']['id']
            else:
                raise Exception('invalid id in event JSON!')

            has_date = 'date' in event['body'] \
                       and event['body']['date'] is not None
            if has_date:
                event['date'] = event['body']['date']
            else:
                raise Exception('invalid date for event JSON!')

            has_night_event = 'nightEvent' in event['body'] \
                              and event['body']['nightEvent'] is not None
            if has_night_event:
                event['night_event'] = event['body']['nightEvent']
            else:
                raise Exception('invalid nightEvent in event JSON!')

            has_source = 'source' in event['body'] \
                         and event['body']['source'] is not None
            if has_source:
                event['source'] = event['body']['source']
            else:
                raise Exception('invalid source in event JSON!')

            has_doses = 'doses' in event['body'] \
                        and event['body']['doses'] is not None
            if has_doses:
                event['doses'] = event['body']['doses']

            has_preemptive = 'preemptive' in event['body'] \
                             and event['body']['preemptive'] is not None
            if has_preemptive:
                event['preemptive'] = event['body']['preemptive']

            has_medication_id = 'medicationId' in event['body'] \
                                and event['body']['medicationId'] is not None
            if has_medication_id:
                event['medication_id'] = event['body']['medicationId']

            has_medication_type = 'medication' in event['body'] \
                                  and event['body']['medication'] is not None \
                                  and 'type' in event['body']['medication'] \
                                  and event['body']['medication']['type'] is not None
            if has_medication_type:
                event['medication_type'] = event['body']['medication']['type']

            # triggers
            has_triggers = 'triggers' in event['body'] \
                           and event['body']['triggers'] is not None \
                           and len(event['body']['triggers']) > 0
            if has_triggers:
                event['triggers'] = []
                for t in event['body']['triggers']:
                    event['triggers'].append(t)

            # symptoms
            has_symptoms = 'symptoms' in event['body'] \
                           and event['body']['symptoms'] is not None \
                           and len(event['body']['symptoms']) > 0
            if has_symptoms:
                event['symptoms'] = []
                for s in event['body']['symptoms']:
                    event['symptoms'].append(s)

            # address
            has_address = 'address' in event['body'] \
                          and event['body']['address'] is not None \
                          and len(event['body']['address']) > 0
            if has_address:
                address = {}
                has_geo = 'latitude' in event['body']['address'] \
                          and event['body']['address']['latitude'] is not None \
                          and 'longitude' in event['body']['address'] \
                          and event['body']['address']['longitude'] is not None
                if has_geo:
                    event['latitude'] = event['body']['address']['latitude']
                    event['longitude'] = event['body']['address']['longitude']

                    has_precision = 'precision' in event['body']['address'] \
                                    and event['body']['address']['precision'] is not None
                    if has_precision:
                        event['precision'] = float(event['body']['address']['precision'])

                has_reverse_geo_code = 'formatted' in event['body']['address'] \
                                       and event['body']['address']['formatted'] is not None
                if has_reverse_geo_code:
                    address['formatted'] =  event['body']['address']['formatted']

                    has_state_code = 'stateCode' in event['body']['address'] \
                                     and event['body']['address']['stateCode'] is not None
                    if has_state_code:
                        address['state_code'] = event['body']['address']['stateCode']
                    else:
                        address['state_code'] = ""

                    has_postal_code = 'postalCode' in event['body']['address'] \
                                      and event['body']['address']['postalCode'] is not None
                    if has_postal_code:
                        address['postal_code'] = event['body']['address']['postalCode']
                    else:
                        address['postal_code'] = ""

                    has_city = 'city' in event['body']['address'] \
                               and event['body']['address']['city'] is not None
                    if has_city:
                        address['city'] = event['body']['address']['city']
                    else:
                        address['city'] = ""

                    has_country = 'country' in event['body']['address'] \
                                  and event['body']['address']['country'] is not None
                    if has_country:
                        address['country'] = event['body']['address']['country']
                    else:
                        address['country'] = ""

                    has_state_or_region = 'stateOrRegion' in event['body']['address'] \
                                          and event['body']['address']['stateOrRegion'] is not None
                    if has_state_or_region:
                        address['state_or_region'] = event['body']['address']['stateOrRegion']
                    else:
                        address['state_or_region'] = ""

                    has_street = 'street' in event['body']['address'] \
                                 and event['body']['address']['street'] is not None
                    if has_street:
                        address['street'] = event['body']['address']['street']
                    else:
                        address['street'] = ""

                    has_street2 = 'street2' in event['body']['address'] \
                                  and event['body']['address']['street2'] is not None
                    if has_street2:
                        address['street2'] = event['body']['address']['street2']
                    else:
                        address['street2'] = ""
                    #NOTE: structs have to be exactly in the order as definite in glue catalog table
                    event['address'] = {
                        'state_code': address['state_code'],
                        'postal_code': address['postal_code'],
                        'city': address['city'],
                        'country': address['country'],
                        'formatted': address['formatted'],
                        'state_or_region': address['state_or_region'],
                        'street': address['street'],
                        'street2': address['street2']
                    }

            # weather
            has_weather = 'weather' in event['body'] \
                          and event['body']['weather'] is not None \
                          and len(event['body']['weather']) > 0
            if has_weather:
                weather = {}
                has_pm25 = 'pm25' in event['body']['weather'] \
                           and event['body']['weather']['pm25'] is not None
                if has_pm25:
                    weather['pm25'] = float(event['body']['weather']['pm25'])

                has_pm10 = 'pm10' in event['body']['weather'] \
                           and event['body']['weather']['pm10'] is not None
                if has_pm10:
                    weather['pm10'] = float(event['body']['weather']['pm10'])

                has_o3 = 'o3' in event['body']['weather'] \
                         and event['body']['weather']['o3'] is not None
                if has_o3:
                    weather['o3'] = float(event['body']['weather']['o3'])

                has_aqi_category = 'aqiCategory' in event['body']['weather'] \
                                   and event['body']['weather']['aqiCategory'] is not None
                if has_aqi_category:
                    weather['aqi_category'] = event['body']['weather']['aqiCategory']

                has_no2 = 'no2' in event['body']['weather'] \
                          and event['body']['weather']['no2'] is not None
                if has_no2:
                    weather['no2'] = float(event['body']['weather']['no2'])

                has_co = 'co' in event['body']['weather'] \
                         and event['body']['weather']['co'] is not None
                if has_co:
                    weather['co'] = float(event['body']['weather']['co'])

                has_so2 = 'so2' in event['body']['weather'] \
                          and event['body']['weather']['so2'] is not None
                if has_so2:
                    weather['so2'] = float(event['body']['weather']['so2'])

                has_aqi = 'aqi' in event['body']['weather'] \
                          and event['body']['weather']['aqi'] is not None
                if has_aqi:
                    weather['aqi'] = int(event['body']['weather']['aqi'])

                has_dew_point = 'dewPoint' in event['body']['weather'] \
                                and event['body']['weather']['dewPoint'] is not None
                if has_dew_point:
                    weather['dew_point'] = int(event['body']['weather']['dewPoint'])

                has_wind_speed = 'windSpeed' in event['body']['weather'] \
                                 and event['body']['weather']['windSpeed'] is not None
                if has_wind_speed:
                    weather['wind_speed'] = int(event['body']['weather']['windSpeed'])

                has_heat_index = 'heatIndex' in event['body']['weather'] \
                                 and event['body']['weather']['heatIndex'] is not None
                if has_heat_index:
                    weather['heat_index'] = int(event['body']['weather']['heatIndex'])

                has_wind_chill = 'windChill' in event['body']['weather'] \
                                 and event['body']['weather']['windChill'] is not None
                if has_wind_chill:
                    weather['wind_chill'] = int(event['body']['weather']['windChill'])

                has_wind_direction = 'windDirection' in event['body']['weather'] \
                                     and event['body']['weather']['windDirection'] is not None
                if has_wind_direction:
                    weather['wind_direction'] = int(event['body']['weather']['windDirection'])

                has_pressure = 'pressure' in event['body']['weather'] \
                               and event['body']['weather']['pressure'] is not None
                if has_pressure:
                    weather['pressure'] = float(event['body']['weather']['pressure'])

                has_humidity = 'humidity' in event['body']['weather'] \
                               and event['body']['weather']['humidity'] is not None
                if has_humidity:
                    weather['humidity'] = int(event['body']['weather']['humidity'])

                has_temperature = 'temperature' in event['body']['weather'] \
                                  and event['body']['weather']['temperature'] is not None
                if has_temperature:
                    weather['temperature'] = int(event['body']['weather']['temperature'])
                #NOTE: Weather is a struct, so it has to be in the EXACT order as the glue catalog table shows
                event['weather'] = {}
                weather_cols= [
                    'pm25',
                    'pm10',
                    'dew_point',
                    'wind_speed',
                    'o3',
                    'aqi_category',
                    'no2',
                    'heat_index',
                    'wind_chill',
                    'co',
                    'wind_direction',
                    'so2',
                    'aqi',
                    'pressure',
                    'humidity',
                    'temperature'
                ]
                for col in weather_cols:
                    if col in weather:
                        event['weather'][col] = weather[col]
                    else:
                        event['weather'][col] = None

            # place
            has_place = 'place' in event['body'] \
                        and event['body']['place'] is not None \
                        and len(event['body']['place']) > 0
            if has_place:
                event['place'] = {
                    'latitude': event['body']['place']['location']['coordinates'][1],
                    'longitude': event['body']['place']['location']['coordinates'][0],
                    'type': event['body']['place']['type'],
                    'name': event['body']['place']['name'],
                    'element': event['body']['place']['element'],
                    'id': event['body']['place']['id']
                }

            # note
            has_note = 'note' in event['body'] \
                       and event['body']['note'] is not None \
                       and len(event['body']['note']) > 0
            if has_note:
                event['note'] = event['body']['note']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_event', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_alert(self, event):
        # alert!
        event['pubSubArgs']= get_pubSubArgs(event)
        try:
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month
            event['alert_id'] = event['body']['_id']
            event['deleted'] = event['body']['deleted']
            event['acknowledged'] = event['body']['acknowledged']
            event['date_created'] = event['body']['dateC']
            event['user_role'] = event['body']['userRole']
            event['user_deleted'] = event['body']['userDeleted']

            # alert_deleted fields, Old alert_deleted don't have updatedAt, default to call_time.
            if event['pubSubEvent'] == 'alert_deleted':
                event['alert_type'] = event['body']['alertType']
                if 'updatedAt' in event['body']:
                    event['date_updated'] = event['body']['updatedAt']
                else:
                    event['date_updated'] = event['call_time']

            # optional params
            has_notified_id = 'notifiedId' in event['body'] \
                              and event['body']['notifiedId'] is not None
            if has_notified_id:
                event['notified_id'] = event['body']['notifiedId']

            has_correlation_id = 'correlationId' in event['body'] \
                                 and event['body']['correlationId'] is not None
            if has_correlation_id:
                event['correlation_id'] = event['body']['correlationId']

            # missed_dose
            has_medication_id = 'medId' in event['body'] \
                                and event['body']['medId'] is not None
            if has_medication_id:
                event['medication_id'] = event['body']['medId']

            has_dose_time = 'dosageTime' in event['body'] \
                            and event['body']['dosageTime'] is not None
            if has_dose_time:
                event['dose_time'] = event['body']['dosageTime']

            # at_risk
            has_first_at_risk_alert = 'firstAtRiskAlert' in event['body'] \
                                      and event['body']['firstAtRiskAlert'] is not None
            if has_first_at_risk_alert:
                event['first_at_risk_alert'] = event['body']['firstAtRiskAlert'] == 'true'

            has_day_uses = 'dayUses' in event['body'] \
                           and event['body']['dayUses'] is not None
            if has_day_uses:
                event['day_uses'] = int(event['body']['dayUses'])

            has_percent_change = 'percentChange' in event['body'] \
                                 and event['body']['percentChange'] is not None
            if has_percent_change:
                event['percent_change'] = int(event['body']['percentChange'])

            # transition
            has_asthma_control_score = 'asthmaScore' in event['body'] \
                                       and event['body']['asthmaScore'] is not None
            if has_asthma_control_score:
                event['asthma_control_score'] = event['body']['asthmaScore']

            # quiet sensor
            has_variables = 'variables' in event['body'] \
                            and not event['body']['variables'] is None
            if has_variables:
                has_mac = 'mac' in event['body']['variables'] \
                          and event['body']['variables']['mac'] is not None
                if has_mac:
                    event['mac'] = event['body']['variables']['mac']

                has_model = 'quietSensor' in event['body']['variables'] \
                            and event['body']['variables']['quietSensor'] is not None \
                            and 'model' in event['body']['variables']['quietSensor'] \
                            and event['body']['variables']['quietSensor']['model'] is not None
                if has_model:
                    event['model'] = event['body']['variables']['quietSensor']['model']

                # custom notifications
                has_comparator = 'comparator' in event['body']['variables'] \
                                 and event['body']['variables']['comparator'] is not None
                if has_comparator:
                    event['comparator'] = event['body']['variables']['comparator']

                has_threshold = 'threshold' in event['body']['variables'] \
                                and event['body']['variables']['threshold'] is not None
                if has_threshold:
                    event['threshold'] = event['body']['variables']['threshold']

                has_window_size = 'window_size' in event['body']['variables'] \
                                  and event['body']['variables']['window_size'] is not None
                if has_window_size:
                    event['window_size'] = event['body']['variables']['window_size']

                has_window_unit = 'window_unit' in event['body']['variables'] \
                                  and event['body']['variables']['window_unit'] is not None
                if has_window_unit:
                    event['window_unit'] = event['body']['variables']['window_unit']

                has_actual = 'actual' in event['body']['variables'] \
                             and event['body']['variables']['actual'] is not None
                if has_actual:
                    event['actual'] = event['body']['variables']['actual']

                has_window_label = 'window_label' in event['body']['variables'] \
                                   and event['body']['variables']['window_label'] is not None
                if has_window_label:
                    event['window_label'] = event['body']['variables']['window_label']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_alert', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_patient_score_updated(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            if 'baseline' in event['body']:
                event['baseline'] = event['body']['baseline']
            else:
                event['baseline'] = DEFAULT_NULL_DOUBLE

            has_data = 'dateR' in event['body'] and event['body']['dateR'] is not None and \
                       'score' in event['body'] and event['body']['score'] is not None
            if has_data:
                event['date'] = event['body']['dateR']
                event['score'] = event['body']['score']
            else:
                print('error in map_patient_score_updated has_data is false.', file=sys.stdout)
                print(json.dumps(event), file=sys.stdout)
                return None

            if 'disease' in event['body'] and event['body']['disease'] is not None:
                event['disease'] = event['body']['disease']
            else:
                event['disease'] = DEFAULT_NULL_STRING

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_patient_score_updated', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_sensor_sync(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month
            event['mac'] = event['body']['mac']
            event['medication_id'] = event['body']['mid']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_sensor_sync', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_app_heartbeat_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            # required fields
            has_app_heartbeat_id = '_id' in event['body'] \
                                   and event['body']['_id'] is not None
            if has_app_heartbeat_id:
                event['app_heartbeat_id'] = event['body']['_id']

            has_platform = 'platform' in event['body'] \
                           and event['body']['platform'] is not None
            if has_platform:
                event['platform'] = event['body']['platform']

            has_rid = 'rid' in event['body'] \
                      and event['body']['rid'] is not None
            if has_rid:
                event['rid'] = event['body']['rid']

            has_app_version = 'app_version' in event['body'] \
                              and event['body']['app_version'] is not None
            if has_app_version:
                event['app_version'] = event['body']['app_version']

            has_model = 'model' in event['body'] \
                        and event['body']['model'] is not None
            if has_model:
                event['model'] = event['body']['model']

            has_date_created = 'dateC' in event['body'] \
                               and event['body']['dateC'] is not None
            if has_date_created:
                event['date_created'] = event['body']['dateC']

            has_packet_type = 'packet_type' in event['body'] \
                              and event['body']['packet_type'] is not None
            if has_packet_type:
                event['packet_type'] = event['body']['packet_type']

            has_signed_in = 'signed_in' in event['body'] \
                            and event['body']['signed_in'] is not None
            if has_signed_in:
                event['signed_in'] = event['body']['signed_in']

            has_timestamp = 'dateC' in event['body'] \
                            and event['body']['dateC'] is not None
            if has_timestamp:
                event['timestamp'] = event['body']['dateC']

            has_device_id = 'device_id' in event['body'] \
                            and event['body']['device_id'] is not None
            if has_device_id:
                event['device_id'] = event['body']['device_id']

            has_os_version = 'os_version' in event['body'] \
                             and event['body']['os_version'] is not None
            if has_os_version:
                event['os_version'] = event['body']['os_version']

            # optional fields
            has_push_on = 'push_on' in event['body'] \
                          and event['body']['push_on'] is not None
            if has_push_on:
                event['push_on'] = event['body']['push_on']

            has_push_type = 'push_type' in event['body'] \
                            and event['body']['push_type'] is not None \
                            and len(event['body']['push_type']) > 0 \
                            and any(event['body']['push_type'])
            if has_push_type:
                event['push_type'] = [i for i in event['body']['push_type'] if i]

            has_network_type = 'network_type' in event['body'] \
                               and event['body']['network_type'] is not None
            if has_network_type:
                event['network_type'] = event['body']['network_type']

            has_latitude = 'lat' in event['body'] \
                           and event['body']['lat'] is not None
            if has_latitude:
                event['latitude'] = event['body']['lat']

            has_longitude = 'lng' in event['body'] \
                            and event['body']['lng'] is not None
            if has_longitude:
                event['longitude'] = event['body']['lng']

            has_location_on = 'location_on' in event['body'] \
                              and event['body']['location_on'] is not None
            if has_location_on:
                event['location_on'] = event['body']['location_on']

            has_location_fix_time = 'location_fix_time' in event['body'] \
                                    and event['body']['location_fix_time'] is not None
            if has_location_fix_time:
                event['location_fix_time'] = event['body']['location_fix_time']

            has_heartbeat_count = 'heartbeat_count' in event['body'] \
                                  and event['body']['heartbeat_count'] is not None
            if has_heartbeat_count:
                event['heartbeat_count'] = event['body']['heartbeat_count']

            has_launch_count = 'launch_count' in event['body'] \
                               and event['body']['launch_count'] is not None
            if has_launch_count:
                event['launch_count'] = event['body']['launch_count']

            has_location_type = 'location_type' in event['body'] \
                                and event['body']['location_type'] is not None
            if has_location_type:
                event['location_type'] = event['body']['location_type']

            has_launch_reason = 'launch_reason' in event['body'] \
                                and event['body']['launch_reason'] is not None
            if has_launch_reason:
                event['launch_reason'] = event['body']['launch_reason']

            has_bluetooth_status = 'bluetooth_status' in event['body'] \
                                   and event['body']['bluetooth_status'] is not None
            if has_bluetooth_status:
                event['bluetooth_status'] = event['body']['bluetooth_status']

            has_app_state = 'app_state' in event['body'] \
                            and event['body']['app_state'] is not None
            if has_app_state:
                event['app_state'] = event['body']['app_state']

            has_precision = 'precision' in event['body'] \
                            and event['body']['precision'] is not None
            if has_precision:
                event['precision'] = float(event['body']['precision'])

            has_location_status = 'location_status' in event['body'] \
                                  and event['body']['location_status'] is not None
            if has_location_status:
                event['location_status'] = event['body']['location_status']

            has_ble_support = 'ble_support' in event['body'] \
                              and event['body']['ble_support'] is not None
            if has_ble_support:
                event['ble_support'] = event['body']['ble_support']

            has_activity_on = 'activity_on' in event['body'] \
                              and event['body']['activity_on'] is not None
            if has_activity_on:
                event['activity_on'] = event['body']['activity_on']

            has_activity_type = 'activity_type' in event['body'] \
                                and event['body']['activity_type'] is not None \
                                and len(event['body']['activity_type']) > 0 \
                                and any(event['body']['activity_type'])
            if has_activity_type:
                event['activity_type'] = [i for i in event['body']['activity_type'] if i]

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_app_heartbeat', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_patient_geo_location_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            event['latitude'] = event['body']['location']['coordinates'][1]
            event['longitude'] = event['body']['location']['coordinates'][0]

            has_precision = 'precision' in event['body'] \
                            and event['body']['precision'] is not None
            if has_precision:
                event['precision'] = float(event['body']['precision'])
            event['date'] = event['body']['dateR']

            # address
            has_address = 'address' in event['body'] \
                          and event['body']['address'] is not None
            if has_address:
                event['address'] = {
                    'street': event['body']['address']['street'],
                    'city': event['body']['address']['city'],
                    'country': event['body']['address']['country'],
                }

                # check & add the optional fields for address globally
                has_postal_code = 'postalCode' in event['body']['address'] \
                                  and event['body']['address']['postalCode'] is not None
                if has_postal_code:
                    event['address']['postal_code'] = event['body']['address']['postalCode']

                has_state_or_region = 'stateOrRegion' in event['body']['address'] \
                                      and event['body']['address']['stateOrRegion'] is not None
                if has_state_or_region:
                    event['address']['state_or_region'] = event['body']['address']['stateOrRegion']

                has_street2 = 'street2' in event['body']['address'] \
                              and event['body']['address']['street2'] is not None
                if has_street2:
                    event['address']['street2'] = event['body']['address']['street2']
                else:
                    event['address']['street2'] = ""

                has_state_code = 'stateCode' in event['body']['address'] \
                                 and event['body']['address']['stateCode'] is not None
                if has_state_code:
                    event['address']['state_code'] = event['body']['address']['stateCode']

            # place
            has_place = 'place' in event['body'] \
                        and event['body']['place'] is not None
            if has_place:
                event['place'] = {
                    'latitude': event['body']['place']['location'][0],
                    'longitude': event['body']['place']['location'][1],
                    'type': event['body']['place']['type'],
                    'name': event['body']['place']['name'],
                    'element': event['body']['place']['element'],
                    'id': event['body']['place']['id']
                }

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_patient_geo_location_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_sensor_message_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            # technically every single field is optional...
            has_type = 'typ' in event['body'] \
                       and event['body']['typ'] is not None
            if has_type:
                event['type'] = event['body']['typ']

            has_raw = 'raw' in event['body'] \
                      and event['body']['raw'] is not None
            if has_raw:
                event['raw'] = event['body']['raw']

            has_device_model = 'device_model' in event['body'] \
                               and event['body']['device_model'] is not None
            if has_device_model:
                event['device_model'] = event['body']['device_model']

            has_system_firmware_version = 'system_firmware_version' in event['body'] \
                                          and event['body']['system_firmware_version'] is not None
            if has_system_firmware_version:
                event['system_firmware_version'] = event['body']['system_firmware_version']

            has_dateR = 'dateR' in event['body'] \
                        and event['body']['dateR'] is not None
            if has_dateR:
                event['date_recorded'] = event['body']['dateR']

            has_date_saved = 'dateS' in event['body'] \
                             and event['body']['dateS'] is not None
            if has_date_saved:
                event['date_saved'] = event['body']['dateS']

            has_mac = 'mac' in event['body'] \
                      and event['body']['mac'] is not None
            if has_mac:
                event['mac'] = event['body']['mac']

            has_bat = 'bat' in event['body'] \
                      and event['body']['bat'] is not None
            if has_bat:
                event['battery'] = event['body']['bat']

            has_frm = 'frm' in event['body'] \
                      and event['body']['frm'] is not None
            if has_frm:
                event['firmware_version'] = event['body']['frm']

            has_mod = 'mod' in event['body'] \
                      and event['body']['mod'] is not None
            if has_mod:
                event['mod'] = event['body']['mod']

            has_evtid = 'evtid' in event['body'] \
                        and event['body']['evtid'] is not None
            if has_evtid:
                event['sensor_event_id'] = event['body']['evtid']

            has_elapsed = 'elapsed' in event['body'] \
                          and event['body']['elapsed'] is not None
            if has_elapsed:
                event['elapsed'] = int(event['body']['elapsed'])

            has_ext_status = 'ext_status' in event['body'] \
                             and event['body']['ext_status'] is not None \
                             and len(event['body']['ext_status']) > 0
            if has_ext_status:
                event['extended_status'] = {}
                ext_status = event['body']['ext_status'][0]

                has_moved = 'moved' in ext_status \
                            and ext_status['moved'] is not None
                if has_moved:
                    event['extended_status']['moved'] = bool(ext_status['moved'])
                else:
                    event['extended_status']['moved'] = False

                has_installed = 'installed' in ext_status \
                                and ext_status['installed'] is not None
                if has_installed:
                    event['extended_status']['installed'] = bool(ext_status['installed'])
                else:
                    event['extended_status']['installed'] = False

                has_orientation = 'orientation' in ext_status \
                                  and ext_status['orientation'] is not None
                if has_orientation:
                    event['extended_status']['orientation'] = bool(ext_status['orientation'])
                else:
                    event['extended_status']['orientation'] = False

                has_silent = 'silent' in ext_status \
                             and ext_status['silent'] is not None
                if has_silent:
                    event['extended_status']['silent'] = bool(ext_status['silent'])
                else:
                    event['extended_status']['silent'] = False

                has_temperature = 'temperature' in ext_status \
                                  and ext_status['temperature'] is not None
                if has_temperature:
                    event['extended_status']['temperature'] = float(ext_status['temperature'])
                else:
                    event['extended_status']['temperature'] = -3000

                has_wake_up = 'wake_up_count' in ext_status \
                              and ext_status['wake_up_count'] is not None
                if has_wake_up:
                    event['extended_status']['wake_up_count'] = int(ext_status['wake_up_count'])
                else:
                    event['extended_status']['wake_up_count'] = -1

                has_awake_time = 'awake_time' in ext_status \
                                 and ext_status['awake_time'] is not None
                if has_awake_time:
                    event['extended_status']['awake_time'] = int(ext_status['awake_time'])
                else:
                    event['extended_status']['awake_time'] = -1

                has_manual_heartbeat = 'manual_heartbeat' in ext_status and \
                                       ext_status['manual_heartbeat'] is not None
                if has_manual_heartbeat:
                    event['extended_status']['manual_heartbeat'] = bool(ext_status['manual_heartbeat'])
                else:
                    event['extended_status']['manual_heartbeat'] = False

                has_reset_cause = 'reset_cause' in ext_status and \
                                  ext_status['reset_cause'] is not None
                if has_reset_cause:
                    event['extended_status']['reset_cause'] = int(ext_status['reset_cause'])
                else:
                    event['extended_status']['reset_cause'] = -1

                has_reset_cause_sw = 'reset_cause_sw' in ext_status and \
                                     ext_status['reset_cause_sw'] is not None
                if has_reset_cause_sw:
                    event['extended_status']['reset_cause_sw'] = int(ext_status['reset_cause_sw'])
                else:
                    event['extended_status']['reset_cause_sw'] = -1

                has_ms_depressed = 'ms_depressed' in ext_status and \
                                   ext_status['ms_depressed'] is not None
                if has_ms_depressed:
                    event['extended_status']['ms_depressed'] = int(ext_status['ms_depressed'])
                else:
                    event['extended_status']['ms_depressed'] = -1

                has_sunfish_state = 'sunfish_state' in ext_status and \
                                    ext_status['sunfish_state'] is not None
                if has_sunfish_state:
                    event['extended_status']['sunfish_state'] = str(ext_status['sunfish_state'])
                else:
                    event['extended_status']['sunfish_state'] = DEFAULT_NULL_STRING

                has_inhale_peak = 'inhale_peak' in ext_status and \
                                  ext_status['inhale_peak'] is not None
                if has_inhale_peak:
                    event['extended_status']['inhale_peak'] = int(ext_status['inhale_peak'])
                else:
                    event['extended_status']['inhale_peak'] = -1

                has_inhale_duration = 'inhale_duration' in ext_status and \
                                      ext_status['inhale_duration'] is not None
                if has_inhale_duration:
                    event['extended_status']['inhale_duration'] = float(ext_status['inhale_duration'])
                else:
                    event['extended_status']['inhale_duration'] = -1.0

                has_removal_threshold = 'removal_threshold' in ext_status and \
                                        ext_status['removal_threshold'] is not None
                if has_removal_threshold:
                    event['extended_status']['removal_threshold'] = int(ext_status['removal_threshold'])
                else:
                    event['extended_status']['removal_threshold'] = -1

                has_event_threshold = 'event_threshold' in ext_status and \
                                      ext_status['event_threshold'] is not None
                if has_event_threshold:
                    event['extended_status']['event_threshold'] = int(ext_status['event_threshold'])
                else:
                    event['extended_status']['event_threshold'] = -1

                has_ir_reference = 'ir_reference' in ext_status and \
                                   ext_status['ir_reference'] is not None
                if has_ir_reference:
                    event['extended_status']['ir_reference'] = int(ext_status['ir_reference'])
                else:
                    event['extended_status']['ir_reference'] = -1

                has_time_since_opened = 'time_since_opened' in ext_status and \
                                        ext_status['time_since_opened'] is not None
                if has_time_since_opened:
                    event['extended_status']['time_since_opened'] = int(ext_status['time_since_opened'])
                else:
                    event['extended_status']['time_since_opened'] = -1

                has_med_removal_count = 'med_removal_count' in ext_status and \
                                        ext_status['med_removal_count'] is not None
                if has_med_removal_count:
                    event['extended_status']['med_removal_count'] = int(ext_status['med_removal_count'])
                else:
                    event['extended_status']['med_removal_count'] = -1

                has_peak_to_mean_ratio = 'peak_to_mean_ratio' in ext_status and \
                                         ext_status['peak_to_mean_ratio'] is not None
                if has_peak_to_mean_ratio:
                    event['extended_status']['peak_to_mean_ratio'] = int(ext_status['peak_to_mean_ratio'])
                else:
                    event['extended_status']['peak_to_mean_ratio'] = -1

                has_peak_count = 'peak_count' in ext_status and \
                                 ext_status['peak_count'] is not None
                if has_peak_count:
                    event['extended_status']['peak_count'] = int(ext_status['peak_count'])
                else:
                    event['extended_status']['peak_count'] = -1

                has_inhale_mean_square = 'inhale_mean_square' in ext_status and \
                                         ext_status['inhale_mean_square'] is not None
                if has_inhale_mean_square:
                    event['extended_status']['inhale_mean_square'] = int(ext_status['inhale_mean_square'])
                else:
                    event['extended_status']['inhale_mean_square'] = -1

                has_press_count = 'press_count' in ext_status and \
                                  ext_status['press_count'] is not None
                if has_press_count:
                    event['extended_status']['press_count'] = int(ext_status['press_count'])
                else:
                    event['extended_status']['press_count'] = -1

                has_shake_duration = 'shake_duration' in ext_status and \
                                     ext_status['shake_duration'] is not None
                if has_shake_duration:
                    event['extended_status']['shake_duration'] = int(ext_status['shake_duration'])
                else:
                    event['extended_status']['shake_duration'] = -1

                has_shake_intensity = 'shake_intensity' in ext_status and \
                                      ext_status['shake_intensity'] is not None
                if has_shake_intensity:
                    event['extended_status']['shake_intensity'] = int(ext_status['shake_intensity'])
                else:
                    event['extended_status']['shake_intensity'] = -1

                has_press_to_inhale = 'press_to_inhale' in ext_status and \
                                      ext_status['press_to_inhale'] is not None
                if has_press_to_inhale:
                    event['extended_status']['press_to_inhale'] = int(ext_status['press_to_inhale'])
                else:
                    event['extended_status']['press_to_inhale'] = -1

                has_average_volume = 'average_volume' in ext_status and \
                                     ext_status['average_volume'] is not None
                if has_average_volume:
                    event['extended_status']['average_volume'] = int(ext_status['average_volume'])
                else:
                    event['extended_status']['average_volume'] = -1

                has_total_volume = 'total_volume' in ext_status and \
                                   ext_status['total_volume'] is not None
                if has_total_volume:
                    event['extended_status']['total_volume'] = int(ext_status['total_volume'])
                else:
                    event['extended_status']['total_volume'] = -1

                has_total_volume_first_second = 'total_volume_first_second' in ext_status and \
                                                ext_status['total_volume_first_second'] is not None
                if has_total_volume_first_second:
                    event['extended_status']['total_volume_first_second'] = int(ext_status['total_volume_first_second'])
                else:
                    event['extended_status']['total_volume_first_second'] = -1

                has_inhale_start_to_peak = 'inhale_start_to_peak' in ext_status and \
                                           ext_status['inhale_start_to_peak'] is not None
                if has_inhale_start_to_peak:
                    event['extended_status']['inhale_start_to_peak'] = int(ext_status['inhale_start_to_peak'])
                else:
                    event['extended_status']['inhale_start_to_peak'] = -1

            has_hub_id = 'hub_id' in event['body'] \
                         and event['body']['hub_id'] is not None
            if has_hub_id:
                event['hub_id'] = event['body']['hub_id']

            has_loc = 'loc' in event['body'] \
                      and event['body']['loc'] is not None \
                      and len(event['body']['loc']) == 2 \
                      and (event['body']['loc'][0] != 0 and event['body']['loc'][1] != 0)
            if has_loc:
                event['latitude'] = event['body']['loc'][0]
                event['longitude'] = event['body']['loc'][1]
                has_precision = 'locP' in event['body'] \
                                and event['body']['locP'] is not None
                if has_precision:
                    event['precision'] = float(event['body']['locP'])

                has_location_method = 'locM' in event['body'] \
                                      and event['body']['locM'] is not None
                if has_location_method:
                    event['location_method'] = event['body']['locM']

                has_location_time = 'loc_time' in event['body'] \
                                    and event['body']['loc_time'] is not None
                if has_location_time:
                    event['location_time'] = event['body']['loc_time']

            has_datP = 'datP' in event['body'] \
                       and event['body']['datP'] is not None
            if has_datP:
                event['date_proxy_received'] = event['body']['datP']

            has_datT = 'datT' in event['body'] \
                       and event['body']['datT'] is not None
            if has_datT:
                event['date_proxy_transmitted'] = event['body']['datT']

            has_mac = 'trans_mac' in event['body'] \
                      and event['body']['trans_mac'] is not None
            if has_mac:
                event['proxy_mac'] = event['body']['trans_mac']

            has_trans_plat = 'trans_plat' in event['body'] \
                             and event['body']['trans_plat'] is not None
            if has_trans_plat:
                event['proxy_platform'] = event['body']['trans_plat']

            has_trans_ver = 'trans_ver' in event['body'] \
                            and event['body']['trans_ver'] is not None
            if has_trans_ver:
                event['proxy_platform_version'] = event['body']['trans_ver']

            has_app_ver = 'app_ver' in event['body'] \
                          and event['body']['app_ver'] is not None
            if has_app_ver:
                event['proxy_app_version'] = event['body']['app_ver']

            has_user_day_id = 'userdayId' in event['body'] \
                              and event['body']['userdayId'] is not None
            if has_user_day_id:
                event['user_day_id'] = event['body']['userdayId']

            has_event_id = 'eventId' in event['body'] \
                           and event['body']['eventId'] is not None
            if has_event_id:
                event['event_id'] = event['body']['eventId']

            has_mid = 'mid' in event['body'] \
                      and event['body']['mid'] is not None
            if has_mid:
                event['medication_id'] = event['body']['mid']

            has_data_link = 'datalink' in event['body'] \
                            and event['body']['datalink'] is not None
            if has_data_link:
                event['data_link'] = event['body']['datalink']
            else:
                event['data_link'] = False

            has_transmission_device_offset = 'tzT' in event['body'] \
                                             and event['body']['tzT'] is not None
            if has_transmission_device_offset:
                event['transmission_device_offset'] = event['body']['tzT']
            else:
                event['transmission_device_offset'] = False

            has_source = 'src' in event['body'] \
                         and event['body']['src'] is not None
            if has_source:
                event['source'] = event['body']['src']
            else:
                event['source'] = False

            has_sensor_message_id = '_id' in event['body'] \
                                    and event['body']['_id'] is not None
            if has_sensor_message_id:
                event['sensor_message_id'] = event['body']['_id']
            else:
                event['sensor_message_id'] = DEFAULT_NULL_STRING

            # optional fields
            has_event_record_status = 'event_record_status' in event['body'] \
                                      and event['body']['event_record_status'] is not None
            if has_event_record_status:
                event['event_record_status'] = event['body']['event_record_status']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_sensor_message_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise


    def map_adherence_updated(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month
            event['doses_expected'] = DEFAULT_NULL_INTEGER
            event['percent'] = DEFAULT_NULL_INTEGER
            event['percent_actual'] = DEFAULT_NULL_INTEGER
            event['date'] = DEFAULT_NULL_DATETIME
            event['meds'] = []

            has_adherence = 'adherence' in event['body'] \
                            and event['body']['adherence'] is not None \
                            and 'dosesTaken' in event['body']['adherence'] \
                            and event['body']['adherence']['dosesTaken'] is not None \
                            and 'dateR' in event['body'] \
                            and event['body']['dateR'] is not None
            if has_adherence:
                event['doses_expected'] = event['body']['adherence']['dosesExpected']
                event['doses_taken'] = event['body']['adherence']['dosesTaken'] \
                    if event['body']['adherence']['dosesTaken'] is not None else 0
                event['doses_taken_actual'] = event['body']['adherence']['dosesTakenActual'] \
                    if event['body']['adherence']['dosesTakenActual'] is not None else 0
                event['percent'] = event['body']['adherence']['percent']
                event['percent_actual'] = event['body']['adherence']['percentActual']
                event['date'] = event['body']['dateR']

                for m in event['body']['adherence']['meds']:
                    event['meds'].append({
                        'medication_id': m['mid'],
                        'doses_expected': m['dosesExpected'],
                        'doses_taken': m['dosesTaken'] if m['dosesTaken'] is not None else 0,
                        'doses_taken_actual': m['dosesTakenActual'] if m['dosesTakenActual'] is not None else 0,
                        'percent': m['percent'],
                        'percent_actual': m['percentActual']
                    })
            else:
                event['bye_bye'] = 'bye'

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']
            return event
        except Exception as e:
            print('error in map_adherence_updated', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(event, file=sys.stdout)
            raise

    def map_shipment_status_updated(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            has_status = 'status' in event['body'] and \
                         event['body']['status'] is not None
            if has_status:
                event['status'] = event['body']['status']

            has_carrier = 'carrier' in event['body'] and \
                          event['body']['carrier'] is not None
            if has_carrier:
                event['carrier'] = event['body']['carrier']

            has_tracking_code = 'trackingCode' in event['body'] and \
                                event['body']['trackingCode'] is not None
            if has_tracking_code:
                event['tracking_code'] = event['body']['trackingCode']

            has_date_shipped = 'dateShipped' in event['body'] and \
                               event['body']['dateShipped'] is not None
            if has_date_shipped:
                event['date_shipped'] = event['body']['dateShipped']

            has_est_delivery_date = 'estDeliveryDate' in event['body'] and \
                                    event['body']['estDeliveryDate'] is not None
            if has_est_delivery_date:
                event['est_delivery_date'] = event['body']['estDeliveryDate']

            has_service_level = 'serviceLevel' in event['body'] and \
                                event['body']['serviceLevel'] is not None
            if has_service_level:
                event['service_level'] = event['body']['serviceLevel']

            has_status_history = 'statusHistory' in event['body'] and \
                                 event['body']['statusHistory'] is not None and \
                                 len(event['body']['statusHistory']) > 0
            if has_status_history:
                event['status_history'] = []
                for s in event['body']['statusHistory']:
                    sh = {}
                    has_status_history_date = 'date' in s and \
                                              s['date'] is not None
                    if has_status_history_date:
                        sh['date'] = s['date']
                    has_status_history_status = 'status' in s and \
                                                s['status'] is not None
                    if has_status_history_status:
                        sh['status'] = s['status']
                    has_status_history_est_delivery_date = 'estDeliveryDate' in s and \
                                                           s['estDeliveryDate'] is not None
                    if has_status_history_est_delivery_date:
                        sh['est_delivery_date'] = s['estDeliveryDate']
                    event['status_history'].append(sh)

            has_mac = 'mac' in event['body'] and \
                      event['body']['mac'] is not None and \
                      len(event['body']['mac']) > 0
            if has_mac:
                event['mac'] = event['body']['mac']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_shipment_status_updated', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_asthma_forecast_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            event['id'] = event['body']['id']
            event['input'] = event['body']['input']
            event['output'] = event['body']['output']
            has_model_version = 'modelVersion' in event['body'] and \
                                event['body']['modelVersion'] is not None
            if has_model_version:
                event['model_version'] = event['body']['modelVersion']
            else:
                event['model_version'] = '1.1.0'

            has_webware_version = 'webwareVersion' in event['body'] and \
                                  event['body']['webwareVersion'] is not None
            if has_webware_version:
                event['webware_version'] = event['body']['webwareVersion']
            else:
                # hardcode to the last webware version that did not pass this through
                event['webware_version'] = '4.35.0'

            has_prediction_date = 'predictionDate' in event['body'] and \
                                  event['body']['predictionDate'] is not None
            if has_prediction_date:
                event['prediction_date'] = event['body']['predictionDate']
            else:
                event['prediction_date'] = datetime_to_iso_str(call_time)

            # parse the input into strongly typed values
            rows = csv.DictReader(event['body']['input'].splitlines())
            field_names = rows.fieldnames
            rows = list(rows)
            float_columns = [
                'co', 'co_max', 'drybulbfahrenheit', 'drybulbfahrenheit_max',
                'lat', 'lon', 'no2', 'no2_max', 'o3', 'o3_max', 'pm10', 'pm10_max',
                'pm25', 'pm25_max', 'relativehumidity', 'relativehumidity_max',
                'so2', 'so2_max', 'stationpressure', 'stationpressure_max',
                'visibility', 'visibility_max', 'wind_speed', 'wind_speed_max',
                'wind_direction'
            ]
            int_columns = [
                'dew_point', 'dew_point_max', 'heat_index', 'heat_index_max',
                'month', 'night_events', 'norm_day', 'prescribed_adh_puffs',
                'rscu_puffs', 'taken_adh_puffs', 'wind_chill', 'wind_chill_max'
            ]
            input_rows = []
            for i in range(0,8):
                input_row = { 'index': i }
                for field_name in field_names:
                    if field_name in float_columns:
                        input_row[field_name] = float(rows[i][field_name])
                    elif field_name in int_columns:
                        input_row[field_name] = int(rows[i][field_name])
                    else:
                        input_row[field_name] = rows[i][field_name]
                input_rows.append(input_row)
            event['input_rows'] = input_rows

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_asthma_forecast_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_group(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["body"]["id"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            event['act_required'] = event['body']['actRequired']
            event['anonymization_method'] = event['body']['anonymizationMethod']
            event['cat_required'] = event['body']['catRequired']
            event['country'] = event['body']['country']
            event['demo_mode_enabled'] = event['body']['demoModeEnabled']
            event['diseases'] = event['body']['diseases']
            event['display_name'] = event['body']['displayName']
            event['fb_tracking'] = event['body']['fbTracking']
            event['hub_allowed'] = event['body']['hubAllowed']
            event['language'] = event['body']['language']
            event['mailing_address_required'] = event['body']['mailingAddressRequired']
            event['name'] = event['body']['name']
            event['preemptive_required'] = event['body']['preemptiveRequired']
            event['provider_enrollment_enabled'] = event['body']['providerEnrollmentEnabled']
            event['self_enrollment_enabled'] = event['body']['selfEnrollmentEnabled']
            event['time_zone'] = event['body']['timeZone']
            event['time_zone_required'] = event['body']['timeZoneRequired']
            event['support_phone'] = event['body']['supportPhone']
            event['support_email'] = event['body']['supportEmail']

            has_additional_user_agreements = 'additionalUserAgreements' in event['body'] and \
                                             event['body']['additionalUserAgreements'] is not None and \
                                             len(event['body']['additionalUserAgreements']) > 0
            if has_additional_user_agreements:
                event['additional_user_agreements'] = []
                for ua in event['body']['additionalUserAgreements']:
                    ua2 = {
                        'adult_filter': ua['adultFilter'],
                        'disease_filter': ua['diseaseFilter'],
                        'medication_filter': ua['medicationFilter'],
                        'file_name': ua['filename']
                    }
                    has_type = 'type' in ua and \
                               ua['type'] is not None
                    if has_type:
                        ua2['type'] = ua['type']
                    else:
                        ua2['type'] = ""
                    event['additional_user_agreements'].append(ua2)

            has_default_data_consent_forms = 'defaultDataConsentForms' in event['body'] and \
                                             event['body']['defaultDataConsentForms'] is not None and \
                                             len(event['body']['defaultDataConsentForms']) > 0
            if has_default_data_consent_forms:
                event['default_data_consent_forms'] = event['body']['defaultDataConsentForms']

            has_default_privacy_policy = 'defaultPrivacyPolicy' in event['body'] and \
                                         event['body']['defaultPrivacyPolicy'] is not None
            if has_default_privacy_policy:
                event['default_privacy_policy'] = event['body']['defaultPrivacyPolicy']

            has_default_terms_and_conditions = 'defaultTermsAndConditions' in event['body'] and \
                                               event['body']['defaultTermsAndConditions'] is not None
            if has_default_terms_and_conditions:
                event['default_terms_and_conditions'] = event['body']['defaultTermsAndConditions']

            has_demographics = 'demographics' in event['body'] and \
                               event['body']['demographics'] is not None and \
                               len(event['body']['demographics']) > 0
            if has_demographics:
                event['demographics'] = event['body']['demographics']

            has_device_encryption_and_passcode = 'deviceEncryptionAndPasscode' in event['body'] and \
                                                 event['body']['deviceEncryptionAndPasscode'] is not None
            if has_device_encryption_and_passcode:
                event['device_encryption_and_passcode'] = event['body']['deviceEncryptionAndPasscode']

            has_max_control_count = 'maxControlCount' in event['body'] and \
                                    event['body']['maxControlCount'] is not None
            if has_max_control_count:
                event['max_control_count'] = event['body']['maxControlCount']

            has_medical_ids_required = 'medicalIdsRequired' in event['body'] and \
                                       event['body']['medicalIdsRequired'] is not None and \
                                       len(event['body']['medicalIdsRequired']) > 0
            if has_medical_ids_required:
                event['medical_ids_required'] = []
                for mi in event['body']['medicalIdsRequired']:
                    mi2 = {
                        'descriptor_key': mi['descriptorKey'],
                        'is_required': mi['isRequired']
                    }
                    has_label = 'label' in mi and \
                                mi['label'] is not None
                    if has_label:
                        mi2['label'] = mi['label']
                    else:
                        mi2['label'] =""
                    has_validator = 'validator' in mi and \
                                    'en-US' in mi['validator'] and \
                                    mi['validator']['en-US'] is not None
                    if has_validator:
                        mi2['validator'] = mi['validator']['en-US']
                    else:
                        mi2['validator'] = ""

                    event['medical_ids_required'].append(mi2)

            has_medication_black_ist = 'medicationBlackList' in event['body'] and \
                                       event['body']['medicationBlackList'] is not None and \
                                       len(event['body']['medicationBlackList']) > 0
            if has_medication_black_ist:
                event['medication_black_list'] = event['body']['medicationBlackList']

            has_medication_white_list = 'medicationWhiteList' in event['body'] and \
                                        event['body']['medicationWhiteList'] is not None and \
                                        len(event['body']['medicationWhiteList']) > 0
            if has_medication_white_list:
                event['medication_white_list'] = event['body']['medicationWhiteList']

            has_order_trigger_strategy = 'orderTriggerStrategy' in event['body'] and \
                                         event['body']['orderTriggerStrategy'] is not None
            if has_order_trigger_strategy:
                event['order_trigger_strategy'] = event['body']['orderTriggerStrategy']

            has_postal_codes_allowed = 'postalCodesAllowed' in event['body'] and \
                                       event['body']['postalCodesAllowed'] is not None and \
                                       len(event['body']['postalCodesAllowed']) > 0
            if has_postal_codes_allowed:
                event['postal_codes_allowed'] = event['body']['postalCodesAllowed']

            has_protocol = 'protocol' in event['body'] and \
                           event['body']['protocol'] is not None
            if has_protocol:
                event['protocol'] = event['body']['protocol']

            has_sensor_black_list = 'sensorBlackList' in event['body'] and \
                                    event['body']['sensorBlackList'] is not None and \
                                    len(event['body']['sensorBlackList']) > 0
            if has_sensor_black_list:
                event['sensor_black_list'] = event['body']['sensorBlackList']

            has_strict_dose_medication_list = 'strictDoseMedicationList' in event['body'] and \
                                              event['body']['strictDoseMedicationList'] is not None and \
                                              len(event['body']['strictDoseMedicationList']) > 0
            if has_strict_dose_medication_list:
                event['strict_dose_medication_list'] = event['body']['strictDoseMedicationList']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_group', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_act_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            has_answered_by_user_id = 'answeredByUid' in event['body'] and \
                                      event['body']['answeredByUid'] is not None
            if has_answered_by_user_id:
                event['answered_by_user_id'] = event['body']['answeredByUid']

            event['score'] = event['body']['score']
            event['answers'] = event['body']['ans']
            event['date_received'] = event['body']['dateR']
            event['date_saved'] = event['body']['dateS']
            event['type'] = event['body']['type']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_act_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_cat_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            has_answered_by_user_id = 'answeredByUid' in event['body'] and \
                                      event['body']['answeredByUid'] is not None
            if has_answered_by_user_id:
                event['answered_by_user_id'] = event['body']['answeredByUid']

            event['score'] = event['body']['score']
            event['answers'] = event['body']['ans']
            event['date_received'] = event['body']['dateR']
            event['date_created'] = event['body']['dateC']

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_cat_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_questionnaire_response_created(self, event):
        try:
            event['pubSubArgs']= get_pubSubArgs(event)
            call_time = try_parsing_date(event['callTime'])

            # flatten out the questionnaire into questions and answers
            questions = []
            answers = []
            for group in event['body']['questionnaire']:
                for question in group['questions']:
                    q = {
                        'question_id': question['questionId'],
                        'answers': [],
                        'group_id': group['questionnaireGroupId'],
                        'text': question['text']
                    }

                    for answer in question['answers']:
                        answer_id = ""
                        has_answer_id = 'answerId' in answer and \
                                        answer['answerId']
                        if has_answer_id:
                            q['answers'].append(answer['answerId'])
                            answer_id = answer['answerId']

                        row_label = ""
                        has_row_label = 'rowLabel' in answer and \
                                        answer['rowLabel'] is not None
                        if has_row_label:
                            row_label = answer['rowLabel']

                        row_label_answer_id = ""
                        has_row_label_answer_id = 'rowLabelAnswerId' in answer and \
                                                  answer['rowLabelAnswerId'] is not None
                        if has_row_label_answer_id:
                            row_label_answer_id = answer['rowLabelAnswerId']

                        col_label = ""
                        has_col_label = 'colLabel' in answer and \
                                        answer['colLabel'] is not None
                        if has_col_label:
                            col_label = answer['colLabel']

                        col_label_answer_id = ""
                        has_col_label_answer_id = 'colLabelAnswerId' in answer and \
                                                  answer['colLabelAnswerId'] is not None
                        if has_col_label_answer_id:
                            col_label_answer_id = answer['colLabelAnswerId']

                        weight = -1.0
                        has_weight = 'weight' in answer and \
                                     answer['weight'] is not None
                        if has_weight:
                            weight = answer['weight']

                        quiz_score = -1
                        has_quiz_score = 'quizScore' in answer and \
                                         answer['quizScore'] is not None
                        if has_quiz_score:
                            quiz_score = answer['quizScore']

                        a = {
                            'answer_id': answer_id,
                            'col_label': col_label,
                            'col_label_answer_id': col_label_answer_id,
                            'question_id': question['questionId'],
                            'quiz_score': quiz_score,
                            'row_label': row_label,
                            'row_label_answer_id': row_label_answer_id,
                            'text': answer['text'],
                            'weight': weight
                        }

                        answers.append(a)

                    questions.append(q)

            resource_id = None
            has_rid = 'rid' in event['body'] and \
                      event['body']['rid'] is not None
            if has_rid:
                resource_id = event['body']['rid']
            time_to_take_questionnaire = None
            has_time_to_take_questionnaire = 'timeToTakeQuestionnaire' in event['body'] and \
                                             event['body']['timeToTakeQuestionnaire'] is not None
            if has_time_to_take_questionnaire:
                time_to_take_questionnaire = event['body']['timeToTakeQuestionnaire']

            event['id'] = event['body']['_id']
            event['answers'] = answers
            event['call_time'] = datetime_to_iso_str(call_time)
            event['date_month'] = call_time.month
            event['date_saved'] = event['body']['dateS']
            event['date_year'] = call_time.year
            event['group_id'] = event["groupId"]

            has_language = 'language' in event['body'] and \
                           event['body']['language'] is not None
            if has_language:
                event['language'] = event['body']['language']
            else:
                event['language'] = '-1'
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['questionnaire_id'] = event['body']['questionnaireId']
            event['questionnaire_name'] = event['body']['questionnaireName']
            event['questionnaire_response_date_created'] = event['body']['questionnaireResponseDateCreated']
            event['questionnaire_response_date_modified'] = event['body']['questionnaireResponseDateModified']
            event['questionnaire_response_id'] = event['body']['questionnaireResponseId']
            event['questionnaire_service'] = event['body']['questionnaireService']
            event['questionnaire_version_id'] = event['body']['questionnaireVersionId']
            event['questions'] = questions
            event['resource_id'] = resource_id
            event['time_to_take_questionnaire'] = time_to_take_questionnaire
            event['user_id'] = event["userId"]

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_questionnaire_response_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_spirometry_session_created(self, event):
        try:
            call_time = try_parsing_date(event['callTime'])
            event['pubSubArgs']= get_pubSubArgs(event)

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            # now the guts
            event['id'] = event['body']['id']
            event['created_date'] = event['body']['createdDate']
            #NOTE: array element structs have to be in exact order as table schema specified in glue catalog
            event['spirometer'] = {
                'manufacturer': event['body']['spirometer']['manufacturer'],
                'custom_data': {},
                'name': event['body']['spirometer']['name'],
                'device_id': event['body']['spirometer']['deviceId']
            }
            has_spirometer_custom_data = 'customData' in event['body']['spirometer'] and \
                                         event['body']['spirometer']['customData'] is not None and \
                                         type(event['body']['spirometer']['customData']) is dict
            if has_spirometer_custom_data:
                for key, value in event['body']['spirometer']['customData'].items():
                    event['spirometer']['custom_data'][str(key)] = str(value)

            event['results'] = []
            for r in event['body']['results']:
                #NOTE: array element structs have to be in exact order as table schema specified in glue catalog
                r2 = {
                    'fvc': float(r['fvc']),
                    'created_date': r['createdDate'],
                }
                has_fev1_over_fvc = 'fev1OverFvc' in r and \
                                    r['fev1OverFvc'] is not None

                if has_fev1_over_fvc:
                    r2['fev1_over_fvc'] = float(r['fev1OverFvc'])
                else:
                    r2['fev1_over_fvc'] = -1.0
                r2['custom_data'] = {}
                has_result_custom_data = 'customData' in r and \
                                         r['customData'] is not None and \
                                         type(r['customData']) is dict
                if has_result_custom_data:
                    for key, value in r['customData'].items():
                        r2['custom_data'][str(key)] = str(value)

                r2['fev1'] = float(r['fev1'])
                r2['fev1_unit'] = r['fev1Unit']
                r2['fvc_unit'] = r['fvcUnit']
                r2['id'] = r['id']
                event['results'].append(r2)

            event['height'] = float(event['body']['height'])
            event['height_unit'] = event['body']['heightUnit']
            event['weight'] = float(event['body']['weight'])
            event['weight_unit'] = event['body']['weightUnit']
            event['custom_data'] ={}
            has_custom_data = 'customData' in event['body'] and \
                              event['body']['customData'] is not None and \
                              type(event['body']['customData']) is dict

            if has_custom_data:
                for key, value in event['body']['customData'].items():
                    event['custom_data'][str(key)] =str(value)

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_spirometry_session_created', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_subgroup(self, event):
        try:
            call_time = try_parsing_date(event['callTime'])
            event['pubSubArgs']= get_pubSubArgs(event)

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            event['subgroup_id'] = event['body'].get('id', DEFAULT_NULL_STRING)
            event['name'] = event['body'].get('name', DEFAULT_NULL_STRING)
            event['display_name'] = event['body'].get('displayName', DEFAULT_NULL_STRING)
            event['type'] = event['body'].get('type', DEFAULT_NULL_STRING)
            event['subgroup_external_ids'] = event['body'].get('externalIds', None)

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_subgroup', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise

    def map_patient_subgroup(self, event):
        try:
            call_time = try_parsing_date(event['callTime'])
            event['pubSubArgs']= get_pubSubArgs(event)

            event['user_id'] = event["userId"]
            event['group_id'] = event["groupId"]
            event['call_time'] = datetime_to_iso_str(call_time)
            event['pub_sub_event_id'] = event['pubSubEventId']
            event['date_year'] = call_time.year
            event['date_month'] = call_time.month

            event['subgroups'] = event['body'].get('subgroups', None)
            event['subgroup_modified'] = event['pubSubArgs'][0]

            del event['userId']
            del event['groupId']
            del event['pubSubEvent']
            del event['body']
            #del event['pubSubArgs']
            del event['callTime']
            del event['pubSubEventId']
            del event['version']

            return event
        except Exception as e:
            print('error in map_patient_subgroup', file=sys.stdout)
            print(type(e), file=sys.stdout)
            print(e.args, file=sys.stdout)
            print(e, file=sys.stdout)
            print(json.dumps(event), file=sys.stdout)
            raise