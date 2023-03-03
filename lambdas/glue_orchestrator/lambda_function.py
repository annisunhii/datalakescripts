import boto3
import time
import sys
# import pytz
from datetime import datetime, timedelta, timezone


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text  # or whatever


def glue_orchestrator(event, context):
    job_state = event['detail']['state']
    job_name = event['detail']['jobName']
    second_level_jobs = [
        'curate-adherence',
        'curate_caregivers',
        'curate-events',
        # 'curate-inference-days',
        # 'curate-patient-hours',
        'curate-patients',
        'curate-providers',
        # 'curate-training-days'
    ]
    if job_state == 'SUCCEEDED' and any(job in job_name for job in second_level_jobs):
        table = remove_prefix([job for job in second_level_jobs if job in job_name][0], 'curate-')
        job_run_id = event['detail']['jobRunId']
        glue = boto3.client('glue')
        job_data = glue.get_job_run(
            JobName=job_name,
            RunId=job_run_id,
            PredecessorsIncluded=True
            )
        print(job_data)
        environment = job_data['JobRun']['Arguments']['--environment']
        bucket = job_data['JobRun']['Arguments']['--bucket']
        job_to_run = 'ph-' + environment + '-redshift-second-level-curated'
        response = glue.start_job_run(
            JobName=job_to_run,
            Arguments={
                '--environment': environment,
                '--bucket': bucket,
                '--table': table,
            }
        )
        print(response)

    if job_state in ['SUCCEEDED', 'FAILED', 'TIMEOUT'] and 'curate-thirdparty' in job_name:
        print(event['detail'])
        job_run_id = event['detail']['jobRunId']
        glue = boto3.client('glue')
        job_data = glue.get_job_run(
            JobName=job_name,
            RunId=job_run_id,
            PredecessorsIncluded=True
            )
        print(job_data)
        environment = job_data['JobRun']['Arguments']['--environment']
        bucket = job_data['JobRun']['Arguments']['--bucket']
        system = job_data['JobRun']['Arguments']['--system']
        component = job_data['JobRun']['Arguments']['--component']
        level = job_data['JobRun']['Arguments']['--level']
        run_type = job_data['JobRun']['Arguments']['--run_type']
        output_format = job_data['JobRun']['Arguments']['--output_format']
        db = 'ph_' + environment + '_db_data_lake'
        error_words = ['concurrent', 'limit', 'quota', 'HeadObject operation: Not Found']
        redshift_job_name = 'ph-' + environment + '-redshift-thirdparty-curated'
        if job_state == 'FAILED':
            error_message = job_data['JobRun']['ErrorMessage']
        if job_state == 'SUCCEEDED' or job_state == 'TIMEOUT' \
                or (job_state == 'FAILED' and any(error in error_message for error in error_words)):
            table_prefix = system + '_'
            table_name = table_prefix + component
            if system == 'google_analytics':
                table_prefix = table_prefix + component + '_'
                table_name = table_prefix + level
                level_string = level + '/'
            else:
                level_string = ''
            s3_path = 's3://' + bucket + '/' \
                      + 'ingress' + '/' \
                      + system + '/' \
                      + output_format + '/' \
                      + component + '/' \
                      + level_string
            today = datetime.strftime(datetime.today(), '%d')
            if run_type == 'full' or today == '1':
                response = glue.get_crawler(
                    Name='ph-' + environment + '-thirdparty-crawler'
                )
                print(response)
                crawler_s3_path = response['Crawler']['Targets']['S3Targets'][0]['Path']
                table_updated_time = None
                try:
                    response = glue.get_table(
                        DatabaseName=db,
                        Name=table_name
                    )
                    print(response)
                    table_updated_time = response['Table']['UpdateTime']
                except glue.exceptions.EntityNotFoundException:
                    pass
                print(table_updated_time)
                print(datetime.now(timezone.utc))
                run_count = 0
                retry = True
                while retry:
                    print(run_count)
                    if s3_path != crawler_s3_path \
                            and (table_updated_time is None or table_updated_time <= datetime.now(timezone.utc)-timedelta(hours=1)):
                        print('s3_path != crawler_s3_path')
                        try:
                            response = glue.delete_table(
                                DatabaseName=db,
                                Name=table_name
                            )
                            print(response)
                        except glue.exceptions.EntityNotFoundException:
                            pass
                        try:
                            response = glue.update_crawler(
                                Name='ph-' + environment + '-thirdparty-crawler',
                                Role='ph-' + environment + '-data-lake-glue-role',
                                DatabaseName=db,
                                Description='string',
                                Targets={
                                    'S3Targets': [
                                        {
                                            'Path': s3_path
                                        },
                                    ]
                                },
                                TablePrefix=table_prefix,
                                SchemaChangePolicy={
                                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                                }
                            )
                            print(response)
                            response = glue.start_crawler(
                                Name='ph-' + environment + '-thirdparty-crawler'
                            )
                            print(response)
                            run_count = run_count + 1
                            retry = False
                        except glue.exceptions.InvalidInputException:
                            run_count = run_count + 1
                            print('crawler error count: ' + str(run_count))
                            time.sleep(30)
                            if run_count < 20:
                                retry = True
                            else:
                                retry = False
                            continue
                    else:
                        break
            curate_thirdparty_job_run_data = glue.get_job_runs(
                JobName=job_name,
                MaxResults=2
            )
            print(curate_thirdparty_job_run_data)
            curate_thirdparty_arguments_list = []
            for job_run in curate_thirdparty_job_run_data['JobRuns']:
                curate_thirdparty_arguments_list.append(job_run['Arguments'])
            curate_thirdparty_job_run_list = []
            for arguments in curate_thirdparty_arguments_list:
                arguments_system = arguments['--system']
                arguments_component = arguments['--component']
                arguments_level = arguments['--level']
                arguments_output_format = arguments['--output_format']
                curate_thirdparty_job_run_list.append(
                    {
                        'system': arguments_system,
                        'component': arguments_component,
                        'level': arguments_level,
                        'output_format': arguments_output_format
                    }
                )
            print(curate_thirdparty_job_run_list)
            if job_state == 'SUCCEEDED':
                if system == 'all' and run_type == 'update':
                    update_run_config_list = [
                        {
                            'system': 'zendesk',
                            'component': 'user',
                            'level': 'all',
                            'output_format': 'parquet'
                        },
                        {
                            'system': 'google_analytics',
                            'component': 'self_enroll',
                            'level': 'session',
                            'output_format': 'csv'
                        },
                        {
                            'system': 'facebook',
                            'component': 'campaign',
                            'level': 'all',
                            'output_format': 'parquet'
                        }
                    ]
                    for run_config in update_run_config_list:
                        temp_dir = "s3://" + bucket + "/glue/temp/curate_thidparty/" \
                                   + run_config['system'] + "/" + run_config['component'] \
                                   + "/" + run_config['level'] + "/"
                        if run_config not in curate_thirdparty_job_run_list:
                            print(run_config)
                            response = glue.start_job_run(
                                JobName=job_name,
                                Arguments={
                                    '--environment': environment,
                                    '--bucket': bucket,
                                    '--system': run_config['system'],
                                    '--component': run_config['component'],
                                    '--level': run_config['level'],
                                    '--run_type': run_type,
                                    '--output_format': run_config['output_format'],
                                    '--TempDir': temp_dir
                                }
                            )
                            print(response)
                elif system != 'all':
                    # redshift_job_name = 'redshift_thirdparty'
                    redshift_thirdparty_curated_job_run_data = glue.get_job_runs(
                        JobName=redshift_job_name,
                        MaxResults=2
                    )
                    print(redshift_thirdparty_curated_job_run_data)
                    redshift_thirdparty_curated_arguments_list = []
                    for job_run in redshift_thirdparty_curated_job_run_data['JobRuns']:
                        redshift_thirdparty_curated_arguments_list.append(job_run['Arguments'])
                    redshift_thirdparty_curated_job_run_list = []
                    for arguments in redshift_thirdparty_curated_arguments_list:
                        arguments_table = arguments.get('--table')
                        redshift_thirdparty_curated_job_run_list.append(
                            {
                                'table': arguments_table
                            }
                        )
                    print(redshift_thirdparty_curated_job_run_list)
                    run_config = {
                        'table': table_name
                    }
                    # if run_config not in redshift_thirdparty_curated_job_run_list:
                    print(run_config)
                    temp_dir = "s3://" + bucket + "/glue/temp/redshift_thirdparty_curated/" + table_name + "/"
                    response = glue.start_job_run(
                        JobName=redshift_job_name,
                        Arguments={
                            '--environment': environment,
                            '--bucket': bucket,
                            '--table': table_name,
                            '--TempDir': temp_dir
                        }
                    )
                    print(response)

                    s3_system_load_done = False
                    next_level = 'all'
                    if system == 'zendesk':
                        component_list = ['user', 'ticket', 'ticket_comment']
                        # if run_type == 'update':
                            # del component_list[2]
                    elif system == 'google_analytics':
                        component_list = ['self_enroll', 'provider_portal', 'ios_app', 'android_app']
                        if level == 'session':
                            next_level = 'event'
                        else:
                            next_level = 'session'
                    elif system =='facebook':
                        component_list = ['campaign', 'ad_set', 'ad', 'ad_insight']
                    elif system == 'dynamodb':
                        component_list = ['sensor_mac']

                    try:
                        print(component_list)
                        if system == 'google_analytics' and level == 'session':
                            next_component = component
                            print(next_component)
                        else:
                            next_component = component_list[component_list.index(component) + 1]
                            print(next_component)
                    except IndexError:
                        s3_system_load_done = True
                        print(system + ' S3 load done - thirdparty on wayne!')
                        exit(0)

                    if not s3_system_load_done:
                        run_config = {
                            'system': system,
                            'component': next_component,
                            'level': next_level,
                            'output_format': output_format
                        }
                        if run_config not in curate_thirdparty_job_run_list:
                            print(run_config)
                            temp_dir = "s3://" + bucket + "/glue/temp/curate_thidparty/" \
                                       + system + "/" + next_component \
                                       + "/" + next_level + "/"
                            response = glue.start_job_run(
                                JobName=job_name,
                                Arguments={
                                    '--environment': environment,
                                    '--bucket': bucket,
                                    '--system': system,
                                    '--component': next_component,
                                    '--level': next_level,
                                    '--run_type': run_type,
                                    '--output_format': output_format,
                                    '--TempDir': temp_dir
                                }
                            )
                            print(response)

        if job_state == 'TIMEOUT' \
                or (job_state == 'FAILED' and any(error in error_message for error in error_words)):
            temp_dir = "s3://" + bucket + "/glue/temp/curate_thidparty/" \
                       + system + "/" + component \
                       + "/" + level + "/"
            response = glue.start_job_run(
                JobName=job_name,
                Arguments={
                    '--environment': environment,
                    '--bucket': bucket,
                    '--system': system,
                    '--component': component,
                    '--level': level,
                    '--run_type': 'update',
                    '--output_format': output_format,
                    '--TempDir': temp_dir
                }
            )
            print(response)
        return
