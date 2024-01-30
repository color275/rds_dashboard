import boto3
import time
import json
from base64 import encode
import os

region = 'ap-northeast-2'
pi_client = boto3.client('pi', region)
rds_client = boto3.client('rds', region)
cw_client = boto3.client('cloudwatch', region)

# 모니터링 인스턴스 식별
# rds intance의 tag key/value가 pi_monitor/true로 마킹된 instance 만 대상으로 식별  
def get_pi_instances():
    response = rds_client.describe_db_instances()
    
    target_instance = []
    
    for instance in response['DBInstances']:
        for tag in instance.get('TagList', []):
            if tag.get('Key') == 'pi_monitor' and tag.get('Value') == 'true':
                target_instance.append(instance)
                break
    return target_instance

# boto3 의 get_resource_metrics 를 통해 metric 수집
def get_resource_metrics(instance, query, start_time, end_time, gather_period):
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
                                'Name':'DBClusterIdentifier',    
                                'Value':get_info['identifier']['dbclusteridentifier']
                            },
                            {
                                'Name':'DBInstanceIdentifier',    
                                'Value':get_info['identifier']['dbinstanceidentifier']
                            }
                        ],
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    }) 
    
    if metric_data:
        try:
            cw_client.put_metric_data(
            Namespace= cloudwatch_namespace,
            MetricData= metric_data)
        except Exception as error:
            raise ValueError('The parameters you provided are incorrect: {}'.format(error))

def main():
    pi_instances = get_pi_instances()

    # 수집할 메트릭 저장 디렉토리
    directory_path = "./metric"
    # CloudWatch Namespace
    cloudwatch_namespace = 'PI-METRIC'
    # 수집 기간
    start_time = time.time() - 600 # 600초 전
    end_time = time.time()
    # inteval (sec)
    gather_period = 60

    # ./metric 디렉토리 내 json 파일 읽기
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)
            
            with open(file_path, 'r') as file:
                metric_queries = json.load(file)

            for instance in pi_instances:
                get_info = get_resource_metrics(instance, metric_queries, start_time, end_time, gather_period)
                if get_info['pi_response']:
                    send_cloudwatch_data(get_info, cloudwatch_namespace)
            
            print(f"Processing {filename}: {len(metric_queries)} metrics Complete!")


if __name__ == "__main__":
	main()