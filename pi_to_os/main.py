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

interval = 600
period = 60

es_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)

def get_pi_instances():
    response = rds_client.describe_db_instances()
    
    target_instance = []
    
    for instance in response['DBInstances']:
        if instance.get('PerformanceInsightsEnabled') :
            for tag in instance.get('TagList', []):
                if tag.get('Key') == 'pi_monitor' and tag.get('Value') == 'true':
                    target_instance.append(instance)
                    break
    return target_instance

# boto3 의 get_resource_metrics 를 통해 metric 수집
def get_resource_metrics(instance, query, start_time, end_time, gather_period):
    try :
        return {
                    'pi_response': pi_client.get_resource_metrics(
                                ServiceType='RDS',
                                Identifier=instance['DbiResourceId'],
                                StartTime=start_time,
                                EndTime=end_time,
                                PeriodInSeconds=gather_period,
                                MetricQueries=query
                                ), 
                    'identifier' : {
                                    'dbclusteridentifier': instance['DBClusterIdentifier'],
                                    'dbinstanceidentifier': instance['DBInstanceIdentifier']
                    }
                }
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
    underbar = string.replace(".","_")
    return underbar


def send_opensearch_group_metric_data(get_info, os_index_nm):
    metric_data = []
    
    # pprint(get_info['pi_response'])
    
    # pprint.pprint(get_info['pi_response']['MetricList'])
    for metric_response in get_info['pi_response']['MetricList']:
        metric_dict = metric_response['Key']
        metric_name = metric_dict['Metric']

        is_metric_dimensions = False
        formatted_dims = []
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']
            
            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append({'Name': replace_dot_to_underbar(key), 'Value': str_encode(metric_dimensions[key])})                
                
                if key == 'db.sql_tokenized.statement' :
                    db_sql_short_statement = textwrap.shorten(metric_dimensions[key], width=150, placeholder='...')
                    # formatted_dims.append({'Name': 'db.sql_short.statement', 'Value': str_encode(db_sql_short_statement)})
                
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
                                                # AdditionalMetrics=[
                                                #                     'db.sql_tokenized.stats.sum_rows_examined_per_call.avg', # 호출당 검사된 행
                                                #                     'db.sql_tokenized.stats.sum_rows_affected_per_call.avg', # 호출당 영향을 받는 행
                                                #                     'db.sql_tokenized.stats.sum_timer_wait_per_call.avg', # 호출당 평균 지연 시간(단위: ms)
                                                #                     'db.sql_tokenized.stats.count_star_per_sec.avg'
                                                #                 ] # 초당 호출 수
                                                
                                                # https://docs.aws.amazon.com/ko_kr/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights.UsingDashboard.AnalyzeDBLoad.AdditionalMetrics.MySQL.html
                                                AdditionalMetrics = [
                                                    'db.sql_tokenized.stats.count_star_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_timer_wait_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_select_full_join_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_select_range_check_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_select_scan_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_sort_merge_passes_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_sort_scan_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_sort_range_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_sort_rows_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_rows_affected_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_rows_examined_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_rows_sent_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_created_tmp_disk_tables_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_created_tmp_tables_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_lock_time_per_sec.avg',
                                                    'db.sql_tokenized.stats.sum_timer_wait_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_select_full_join_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_select_range_check_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_select_scan_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_sort_merge_passes_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_sort_scan_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_sort_range_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_sort_rows_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_rows_affected_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_rows_examined_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_rows_sent_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_created_tmp_disk_tables_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_created_tmp_tables_per_call.avg',
                                                    'db.sql_tokenized.stats.sum_lock_time_per_call.avg',
                                                ]
                                            )
                    # pprint.pprint(query_metric_response)
                    query_metric_dimensions_list = query_metric_response['Keys']
                    for query_metric_dimensions in query_metric_dimensions_list :
                        if 'AdditionalMetrics' in query_metric_dimensions :
                            for key in query_metric_dimensions['AdditionalMetrics'] :
                                # db.sql_tokenized.stats.count_star_per_sec.avg
                                formatted_dims.append({'Name': replace_dot_to_underbar(key), 'Value': query_metric_dimensions['AdditionalMetrics'][key]})

            formatted_dims.append({'Name': 'DBClusterIdentifier', 'Value': get_info['identifier']['dbclusteridentifier']})
            formatted_dims.append({'Name': 'DBInstanceIdentifier', 'Value': get_info['identifier']['dbinstanceidentifier']})
            is_metric_dimensions = True

        # pprint.pprint(formatted_dims)
        for datapoint in metric_response['DataPoints']:
            value = datapoint.get('Value', None)
            if value:
                if is_metric_dimensions:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': formatted_dims,
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    })
                # else:
                #     metric_data.append({
                #         'MetricName': metric_name,
                #         'Dimensions': [
                #             {
                #                 'Name':'DBClusterIdentifier',    
                #                 'Value':get_info['identifier']['dbclusteridentifier']
                #             },
                #             {
                #                 'Name': 'DBInstanceIdentifier',
                #                 'Value': get_info['identifier']['dbinstanceidentifier']
                #             }
                #         ],
                #         'Timestamp': datapoint['Timestamp'],
                #         'Value': round(datapoint['Value'], 2)
                #     }) 
    
    if metric_data:
        try:
            for metric in metric_data:
                document = {
                    'timestamp': metric['Timestamp'].isoformat(),
                    'metric_name': metric['MetricName'],
                    'value': metric['Value']
                }
                if metric['Dimensions']:
                    for dim in metric['Dimensions']:
                        document[dim['Name']] = dim['Value']
                
                
                # pprint(document)
                es_client.index(
                    index=os_index_nm,
                    body=document
                )
        except Exception as error:
            raise ValueError('Failed to send data to OpenSearch: {}'.format(error))
    else:
        pass


def main():
    pi_instances = get_pi_instances()

    # 수집할 메트릭 저장 디렉토리
    directory_path = "./metric"
    # CloudWatch Namespace
    os_index_nm = 'rds-monitor-pi-sql-1'
    # 수집 기간
    start_time = time.time() - 60 # 600초 전
    end_time = time.time()
    # inteval (sec)
    gather_period = 60
    

    try :
        # ./metric 디렉토리 내 json 파일 읽기
        for filename in os.listdir(directory_path):
            if filename.endswith(".json"):
                file_path = os.path.join(directory_path, filename)
                
                if os.path.getsize(file_path) > 0:
                    with open(file_path, 'r') as file:
                        metric_queries = json.load(file)

                    for instance in pi_instances:
                        get_info = get_resource_metrics(instance, metric_queries, start_time, end_time, gather_period)
                        if get_info['pi_response']:
                            send_opensearch_group_metric_data(get_info, os_index_nm)
                    
                    print(f"Processing {filename}: {len(metric_queries)} metrics Complete!")
    except Exception as e:
        print(f"error : {e}")

if __name__ == "__main__":
	main()