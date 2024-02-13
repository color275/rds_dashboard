import boto3
import time
import json
from base64 import encode
import os
from botocore.exceptions import ClientError
# from pprint import pprint

region = 'ap-northeast-2'
pi_client = boto3.client('pi', region)
rds_client = boto3.client('rds', region)
cw_client = boto3.client('cloudwatch', region)

# 모니터링 인스턴스 식별
# rds intance의 tag key/value가 pi_monitor/true로 마킹된 instance 만 대상으로 식별  
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
# https://docs.aws.amazon.com/ko_kr/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights_Counters.html#USER_PerfInsights_Counters.OS
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

        # return {
        #             'pi_response': pi_client.get_resource_metrics(
        #                         ServiceType='RDS',
        #                         Identifier=instance['DbiResourceId'],
        #                         StartTime=start_time,
        #                         EndTime=end_time,
        #                         PeriodInSeconds=gather_period,
        #                         MetricQueries=query
        #                         ), 
        #             'identifier' : {
        #                             'dbclusteridentifier': instance['DBClusterIdentifier'],
        #                             'dbinstanceidentifier': instance['DBInstanceIdentifier']
        #             }
        #         }
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

# 전처리된 메트릭을 cloudwatch 로 전송
def send_cloudwatch_data(get_info, cloudwatch_namespace):
    
    metric_data = []
    for metric_response in get_info['pi_response']['MetricList']:
        metric_dict = metric_response['Key']
        metric_name = metric_dict['Metric']
     
        is_metric_dimensions = False
        formatted_dims = []
        
        # Dimensions 포함된 메트릭
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']  # return a dictionary
            
            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key])))
                
            formatted_dims.append(dict(Name='DBClusterIdentifier', Value=get_info['identifier']['dbclusteridentifier']))
            formatted_dims.append(dict(Name='DBInstanceIdentifier', Value=get_info['identifier']['dbinstanceidentifier']))
            is_metric_dimensions = True
       
        for datapoint in metric_response['DataPoints']:
            value = datapoint.get('Value', 0)
            if is_metric_dimensions:
                metric_data.append({
                    'MetricName': metric_name,
                    'Dimensions': formatted_dims,
                    'Timestamp': datapoint['Timestamp'],
                    'Value': round(value,2)
                })
            else:
                metric_data.append({
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name':'DBClusterIdentifier',    
                            'Value':get_info['identifier']['dbclusteridentifier']
                        },
                        {
                            'Name':'DBInstanceIdentifier',    
                            'Value':get_info['identifier']['dbinstanceidentifier']
                        }
                    ],
                    'Timestamp': datapoint['Timestamp'],
                    'Value': round(value,2)
                }) 
    
    if metric_data:
        try:
            cw_client.put_metric_data(
            Namespace= cloudwatch_namespace,
            MetricData= metric_data)
        except Exception as error:
            raise ValueError('The parameters you provided are incorrect: {}'.format(error))

def main():

    ########################################
    ## Variable
    ########################################
    directory_path = "./metric"
    cloudwatch_namespace = 'PI-METRIC-CHIHOLEE'
    start_time = time.time() - 600 
    end_time = time.time()
    gather_period = 60
    tag_key = "pi_monitor"
    tag_value = "true"
    #########################################
    
    pi_instances = get_pi_instances(tag_key, tag_value)
    
    for filename in os.listdir(directory_path):
        
        if filename == 'test.json' :
            continue
        
        if filename.endswith(".json"):
            
            file_path = os.path.join(directory_path, filename)
            
            if os.path.getsize(file_path) > 0:
                with open(file_path, 'r') as file:
                    metric_queries = json.load(file)

                for instance in pi_instances:
                    all_metrics = get_resource_metrics(instance, metric_queries, start_time, end_time, gather_period)
                    for get_info in all_metrics :
                        if get_info['pi_response']:
                            send_cloudwatch_data(get_info, cloudwatch_namespace)
                
                print(f"Processing {filename}: {len(metric_queries)} metrics Complete!")

if __name__ == "__main__":
	main()