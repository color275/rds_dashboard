import boto3
import time
import datetime
import pprint
import json
from base64 import encode
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import textwrap
import os

region = 'ap-northeast-2'

pi_client = boto3.client('pi', region)
rds_client = boto3.client('rds', region)
cw_client = boto3.client('cloudwatch', region)

host = 'vpc-test-aponilxfo5qn2nfe6mitxf2rxu.ap-northeast-2.es.amazonaws.com' # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com
service = 'es'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

interval = 600
period = 60


def get_pi_instances():
    response = rds_client.describe_db_instances()
    
    target_instance = []
    
    for instance in response['DBInstances']:
        for tag in instance.get('TagList', []):
            if tag.get('Key') == 'monitor' and tag.get('Value') == 'true':
                target_instance.append(instance)
                break
    return target_instance

def get_resource_metrics(instance, query):

    return {
                'pi_response': pi_client.get_resource_metrics(
                             ServiceType='RDS',
                             Identifier=instance['DbiResourceId'],
                             StartTime=time.time() - interval,
                             EndTime=time.time(),
                             PeriodInSeconds=period,
                             MetricQueries=query
                             ), 
                'dbinstanceidentifier': instance['DBInstanceIdentifier']
            }

def remove_non_ascii(string):
    non_ascii = ascii(string)
    return non_ascii

def str_encode(string):
    encoded_str = string.encode("ascii","ignore")
    return remove_non_ascii(encoded_str.decode())

def send_cloudwatch_data(get_info):
    
    metric_data = []
    
    # pprint.pprint(get_info['pi_response']['MetricList'])
    for metric_response in get_info['pi_response']['MetricList']: #dataoints and key
        metric_dict = metric_response['Key']  #db.load.avg
        metric_name = metric_dict['Metric']

     
        is_metric_dimensions = False
        formatted_dims = []
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']  # return a dictionary
            
            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key])))

                if key == 'db.sql_tokenized.statement' :
                    db_sql_short_statement = textwrap.shorten(metric_dimensions[key], width=150, placeholder='...')
                    formatted_dims.append({'Name': 'db.sql_short.statement', 'Value': str_encode(db_sql_short_statement)})

                if key == 'db.sql_tokenized.id' :
                    db_sql_tokenized_id = metric_dimensions[key]
                    db_resource_id = get_info['pi_response']['Identifier']

                    query_metric_response =  pi_client.describe_dimension_keys(
                                                ServiceType='RDS',
                                                Identifier=db_resource_id,
                                                StartTime=time.time() - interval,
                                                EndTime=time.time(),
                                                Metric="db.load.avg",
                                                PeriodInSeconds=period,
                                                GroupBy={
                                                    'Group': 'db.sql_tokenized'
                                                },
                                                Filter={
                                                    'db.sql_tokenized.id': 	db_sql_tokenized_id
                                                },
                                                AdditionalMetrics=['db.sql_tokenized.stats.sum_rows_examined_per_call.avg', # 호출당 검사된 행
                                                                'db.sql_tokenized.stats.sum_rows_affected_per_call.avg', # 호출당 영향을 받는 행
                                                                'db.sql_tokenized.stats.sum_timer_wait_per_call.avg', # 호출당 평균 지연 시간(단위: ms)
                                                                'db.sql_tokenized.stats.count_star_per_sec.avg'] # 초당 호출 수
                                            )
                    # pprint.pprint(query_metric_response)
                    query_metric_dimensions_list = query_metric_response['Keys']
                    for query_metric_dimensions in query_metric_dimensions_list :                        
                        if 'AdditionalMetrics' in query_metric_dimensions :
                            for key in query_metric_dimensions['AdditionalMetrics'] :
                                # db.sql_tokenized.stats.count_star_per_sec.avg
                                # type: <class 'float'>, valid types: <class 'str'>
                                formatted_dims.append({'Name': key, 'Value': str(query_metric_dimensions['AdditionalMetrics'][key])})
               

            formatted_dims.append(dict(Name='DBInstanceIdentifier', Value=get_info['dbinstanceidentifier']))
            is_metric_dimensions = True
        else:
            pass
            # metric_name = metric_name.replace("avg","")

        for datapoint in metric_response['DataPoints']:
            # We don't always have values from an instance
            value = datapoint.get('Value', None)
            if value:
                if is_metric_dimensions:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': formatted_dims,
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    })
                else:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': [
                            {
                                'Name':'DBInstanceIdentifier',    
                                'Value':get_info['dbinstanceidentifier']
                            } 
                        ],
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    }) 
    
    if metric_data:
        # logger.info('## sending data to cloduwatch...')
        try:
            cw_client.put_metric_data(
            Namespace= 'PI-TEST3',
            MetricData= metric_data)
        except ClientError as error:
            raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    else:
        # logger.info('## NO Metric Data ##')
        pass


pi_instances = get_pi_instances()

directory_path = "./metric"


for filename in os.listdir(directory_path):
    if filename.endswith(".json"):
        file_path = os.path.join(directory_path, filename)
        
        with open(file_path, 'r') as file:
            metric_queries = json.load(file)

        for instance in pi_instances:
            get_info = get_resource_metrics(instance, metric_queries)

            # pprint.pprint(get_info)
            if get_info['pi_response']:
                send_cloudwatch_data(get_info)
        
        print(f"Processing {filename}: {len(metric_queries)} metrics Complete!")