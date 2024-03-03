# PI-SQL-CHIHOLEE
import boto3
import time
import datetime
from pprint import pprint
import json
from base64 import encode
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import textwrap
import os
from botocore.exceptions import ClientError

region = 'ap-northeast-2'

pi_client = boto3.client('pi', region)
rds_client = boto3.client('rds', region)
cw_client = boto3.client('cloudwatch', region)

host = 'vpc-test-aponilxfo5qn2nfe6mitxf2rxu.ap-northeast-2.es.amazonaws.com'
service = 'es'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)


es_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)

def get_pi_instances(tag_key, tag_value):
    response = rds_client.describe_db_instances()
    
    target_instance = []
    
    for instance in response['DBInstances']:
        if instance.get('PerformanceInsightsEnabled') :
            for tag in instance.get('TagList', []):
                if tag.get('Key') == tag_key and tag.get('Value') == tag_value:
                    target_instance.append(instance)
                    break
    return target_instance

# boto3 의 get_resource_metrics 를 통해 metric 수집
def get_resource_metrics(instance, query, start_time, end_time, gather_period):
    all_metrics = []
    next_token = None

    try :
        while True :
            if next_token :
                response = pi_client.get_resource_metrics(
                                ServiceType='RDS',
                                Identifier=instance['DbiResourceId'],
                                StartTime=start_time,
                                EndTime=end_time,
                                PeriodInSeconds=gather_period,
                                MetricQueries=query,
                                NextToken=next_token
                                )
            else :
                response = pi_client.get_resource_metrics(
                                ServiceType='RDS',
                                Identifier=instance['DbiResourceId'],
                                StartTime=start_time,
                                EndTime=end_time,
                                PeriodInSeconds=gather_period,
                                MetricQueries=query
                                )
            
            response_dict = {
                'pi_response' : response,
                'identifier' : {
                                'dbclusteridentifier': instance['DBClusterIdentifier'],
                                'dbinstanceidentifier': instance['DBInstanceIdentifier']
                }
            }
            all_metrics.append(response_dict)
            next_token = response.get('NextToken', None)

            if not next_token :
                break
        
        return all_metrics

    except ClientError as error:
        print(f"Error...")
        # 오류 발생 시, 오류 정보를 포함한 응답 반환
        return {
            'pi_response': None, 
            'identifier': {
                'dbclusteridentifier': instance.get('DBClusterIdentifier', 'N/A'),
                'dbinstanceidentifier': instance['DBInstanceIdentifier'],
                'error': str(error)
            }
        }


def remove_non_ascii(string):
    non_ascii = ascii(string)
    return non_ascii

def str_encode(string):
    encoded_str = string.encode("ascii","ignore")
    return remove_non_ascii(encoded_str.decode())

def replace_dot_to_underbar(string):
    underbar = string.replace(".","_").lower()
    return underbar

def remove_quotes(string):    
    if string.startswith("'") and string.endswith("'"):
        return string[1:-1]
    return string


def send_opensearch_group_metric_data(get_info, os_index_nm, start_time, end_time, gather_period):
    metric_data = []
    
    for metric_response in get_info['pi_response']['MetricList']:
        metric_dict = metric_response['Key']
        metric_name = metric_dict['Metric']

        is_metric_dimensions = False
        formatted_dims = []
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']
            
            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append({'Name': replace_dot_to_underbar(key), 'Value': remove_quotes(str_encode(metric_dimensions[key]))})
                
                if key == 'db.sql_tokenized.id' :
                    db_sql_tokenized_id = metric_dimensions['db.sql_tokenized.id']
                    db_resource_id = get_info['pi_response']['Identifier']                    

                    query_metric_response = pi_client.describe_dimension_keys(
                                                            ServiceType='RDS',
                                                            Identifier=db_resource_id,
                                                            StartTime=start_time,
                                                            EndTime=end_time,
                                                            Metric="db.load.avg",
                                                            PeriodInSeconds=gather_period,
                                                            GroupBy={
                                                                'Group': 'db.sql'
                                                            },
                                                            Filter={
                                                                'db.sql_tokenized.id': 	db_sql_tokenized_id
                                                            },
                                                            MaxResults=1
                            
                                            )


                    if query_metric_response.get('Keys') :
                        for dim in query_metric_response['Keys'] :
                            db_sql_id = dim['Dimensions']['db.sql.id']
                            db_sql_db_id = dim['Dimensions']['db.sql.db_id']
                            break

                    query_metric_response = pi_client.get_dimension_key_details(
                                                            ServiceType='RDS',
                                                            Identifier=db_resource_id,
                                                            Group='db.sql',
                                                            GroupIdentifier=db_sql_id,
                                                            RequestedDimensions=[
                                                                'db.sql.statement'
                                                            ]
                                                        )
                    

                    if query_metric_response.get('Dimensions') :
                        for query in query_metric_response['Dimensions'] :
                            if query.get('Value') :
                                sql_fulltext = query.get('Value')
                                sql_short_fulltext = textwrap.shorten(sql_fulltext, width=150, placeholder='...')
                                break
                    
                    query_metric_response =  pi_client.describe_dimension_keys(
                                                ServiceType='RDS',
                                                Identifier=db_resource_id,
                                                StartTime=time.time() - 300,
                                                EndTime=time.time(),
                                                Metric="db.load.avg",
                                                PeriodInSeconds=gather_period,
                                                GroupBy={
                                                    'Group': 'db.sql_tokenized',
                                                },
                                                Filter={
                                                    'db.sql_tokenized.id': 	db_sql_tokenized_id
                                                },
                                                
                                                # https://docs.aws.amazon.com/ko_kr/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights.UsingDashboard.AnalyzeDBLoad.AdditionalMetrics.MySQL.html
                                                AdditionalMetrics = [
                                                    'db.sql_tokenized.stats.sum_created_tmp_tables_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_lock_time_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_lock_time_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_rows_examined_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_rows_examined_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_rows_sent_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_rows_sent_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_timer_wait_per_sec.avg',
                                                    # 초당 호출 수
                                                    'db.sql_tokenized.stats.count_star_per_sec.avg',
                                                    # 호출당 평균 지연 시간(단위: ms)
                                                    'db.sql_tokenized.stats.sum_timer_wait_per_call.avg',
                                                ],
                                            )
                    
                    query_metric_dimensions_list = query_metric_response['Keys']
                    for query_metric_dimensions in query_metric_dimensions_list :
                        if 'AdditionalMetrics' in query_metric_dimensions :
                            for key in query_metric_dimensions['AdditionalMetrics'] :
                                formatted_dims.append({'Name': replace_dot_to_underbar(key), 'Value': query_metric_dimensions['AdditionalMetrics'][key]})

            formatted_dims.append({'Name': 'cluster_name', 'Value': get_info['identifier']['dbclusteridentifier']})
            formatted_dims.append({'Name': 'instance_name', 'Value': get_info['identifier']['dbinstanceidentifier']})
            formatted_dims.append({'Name': 'sql_fulltext', 'Value': sql_fulltext})
            formatted_dims.append({'Name': 'sql_short_fulltext', 'Value': sql_short_fulltext})
            formatted_dims.append({'Name': 'db_sql_id', 'Value': db_sql_id})
            formatted_dims.append({'Name': 'db_sql_db_id', 'Value': db_sql_db_id})
            is_metric_dimensions = True

        for datapoint in metric_response['DataPoints']:
            value = datapoint.get('Value', 0)
            if is_metric_dimensions:
                metric_data.append({
                    'MetricName': metric_name,
                    'Dimensions': formatted_dims,
                    'Timestamp': datapoint['Timestamp'],
                    'Value': round(value, 2)
                })

    if metric_data:
        try:
            for metric in metric_data:
                document = {
                    'timestamp': metric['Timestamp'].isoformat(),
                    'metric_name': metric['MetricName'],
                }
                if metric['Dimensions']:
                    for dim in metric['Dimensions']:
                        document[dim['Name']] = dim['Value']
                
                es_client.index(
                    index=os_index_nm,
                    body=document
                )
        except Exception as error:
            raise ValueError('Failed to send data to OpenSearch: {}'.format(error))
    else:
        pass


def lambda_handler(event, context):

    ########################################
    ## Variable
    ########################################
    directory_path = "./metric"
    os_index_nm = 'rds-monitor-pi-sql-2'
    start_time = time.time() - 60
    end_time = time.time()
    gather_period = 60
    tag_key = "pi_monitor"
    tag_value = "true"
    ########################################
    
    pi_instances = get_pi_instances(tag_key, tag_value)

    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)
            
            if os.path.getsize(file_path) > 0:
                with open(file_path, 'r') as file:
                    metric_queries = json.load(file)

                for instance in pi_instances:
                    all_metrics = get_resource_metrics(instance, metric_queries, start_time, end_time, gather_period)
                    for get_info in all_metrics :
                        if get_info['pi_response']:
                            send_opensearch_group_metric_data(get_info, os_index_nm, start_time, end_time, gather_period)
                
                print(f"Processing {filename}: {len(metric_queries)} metrics Complete!")
    return {
        'statusCode': 200,
        'body': json.dumps('Finish!')
    }



test_event = {}
test_context = {}

if __name__ == "__main__":
    response = lambda_handler(test_event, test_context)