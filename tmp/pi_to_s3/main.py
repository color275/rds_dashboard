import boto3
import time
from datetime import datetime
from base64 import encode
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd 
import pytz 
from pprint import pprint
import sys


region = 'ap-northeast-2'

pi_client = boto3.client('pi', region)
s3 = boto3.client('s3', region_name=region)
rds_client = boto3.client('rds', region)
cw_client = boto3.client('cloudwatch', region)
dynamodb = boto3.resource('dynamodb', region_name=region)

dynamodb_table = dynamodb.Table('SqlTokenizedTable') 

korea_tz = pytz.timezone('Asia/Seoul')

db_identifier_list = [
                        'db-IUJELG26COMQKPV7RDTERN3WR4',
                        'db-ZIBJAVYAOHMU2UHYNWTAVXHWNY'
                    ]

def get_sql_detail(db_identifier, groupidentifier) :
    reponse = pi_client.get_dimension_key_details(
        ServiceType='RDS',
        Identifier=db_identifier,
        Group='db.sql',
        GroupIdentifier=groupidentifier,
        RequestedDimensions=[
            'db.sql.statement'
        ]
    )
    for metric in reponse['Dimensions'] :
        if metric.get('Value') :
            sql = metric.get('Value')
            break
    return sql    

def get_sql(db_identifier) :
    response = pi_client.get_resource_metrics(
        ServiceType='RDS',
        Identifier=db_identifier,
        MetricQueries=[
            {
            "Metric": "db.load.avg",
            "GroupBy": {
                "Group": "db.sql"
            }
        }
        ],
        StartTime=time.time() - (70),
        EndTime=time.time(),
        PeriodInSeconds=1
    )
    return response

db_identifier_dict = {}

try:
    for db_identifier in db_identifier_list:
        # 모든 RDS 인스턴스 정보 조회
        response = rds_client.describe_db_instances()
        
        # 조회된 인스턴스들 중 원하는 리소스 ID를 가진 인스턴스 찾기
        found = False
        for db_instance in response['DBInstances']:
            if db_instance['DbiResourceId'] == db_identifier:
                found = True
                # 원하는 리소스 ID를 가진 인스턴스의 DB 인스턴스 ID(이름) 입력
                db_identifier_dict[db_identifier] = db_instance['DBInstanceIdentifier']
                print(f"DB Instance ID (Name): {db_instance['DBInstanceIdentifier']}")
                
                # Performance Insights 활성화 여부 확인
                if not db_instance.get('PerformanceInsightsEnabled', False):
                    print(f"Performance Insights is disabled for instance: {db_instance['DBInstanceIdentifier']}")
                    sys.exit(1)  # Performance Insights가 비활성화된 경우 프로그램 종료
                break
                
        if not found:
            # 일치하는 리소스 ID를 가진 인스턴스가 없는 경우
            print(f"No matching RDS instance found for the provided resource ID: {db_identifier}")
            sys.exit(1)
except Exception as e:
    print(f"Error fetching RDS instances: {e}")
    sys.exit(1)
    


for db_identifier, db_instance_name in db_identifier_dict.items() :
    sql_list = []
    full_text_list = []

    response = get_sql(db_identifier)
    
    for metric_response in response['MetricList'] :

        metric_dict = metric_response['Key']
        if metric_dict.get('Dimensions') :
            sql_info = metric_dict['Dimensions']
            db_sql_id = sql_info['db.sql.id']
            db_sql_statement = sql_info['db.sql.statement']
            db_sql_tokenized_id = sql_info['db.sql.tokenized_id']        
            
            v = 0
            before_v = 0
            timestamp = ""
            if metric_response['DataPoints'] :
                datapoints = metric_response['DataPoints']
                for datapoint in datapoints :               
                    
                    if datapoint.get('Value') :
                        before_v = datapoint['Value']

                        if v is None or v < before_v :
                            v = before_v
                            timestamp = datapoint['Timestamp']
                    
            
    #         print(f"""
    # # db_sql_id : {db_sql_id}
    # # db_sql_tokenized_id : {db_sql_tokenized_id}
    # # timestamp : {timestamp}
    # # cpu_load : {v}
    # # db_sql_statement : {db_sql_statement}
    #         """)

            
            try :
                # dynamodb 에 저장. db_sql_tokenized_id가 존재하지 않을 경우에만 추가
                current_time = datetime.now().astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S")
                dynamodb_table.put_item(
                    Item={
                        'db_sql_tokenized_id': db_sql_tokenized_id,
                        'db_identifier': db_identifier,
                        'db_instance_name': db_instance_name,
                        'last_update_time': current_time
                    },
                    ConditionExpression='attribute_not_exists(db_sql_tokenized_id) AND attribute_not_exists(db_identifier)'  
                )

                data = {
                    "db_sql_id": db_sql_id,
                    "db_sql_tokenized_id": db_sql_tokenized_id,
                    "db_identifier": db_identifier,
                    "db_instance_name": db_instance_name,
                    # "last_update_time": timestamp.astimezone(korea_tz).strftime("%Y-%m-%d.%H:%M:%S"),
                    "last_update_time": timestamp.astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S"),
                    "cpu_load": v
                }       
                
                sql_list.append(data)

                data_detail = {
                    "db_sql_id": db_sql_id,
                    "db_identifier": db_identifier,
                    "db_instance_name": db_instance_name,
                    "sql_fulltext": get_sql_detail(db_identifier, db_sql_id),
                    "last_update_time": timestamp.astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S")
                }

                full_text_list.append(data_detail)
            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                # db_sql_tokenized_id가 이미 존재하는 경우 예외 처리
                # print(f"Skipped existing db_sql_tokenized_id: {db_sql_tokenized_id}")
                pass
            
            

    if sql_list:
        # 데이터를 Parquet 형식으로 변환
        df = pd.DataFrame(sql_list)

        # 날짜 및 시간 포맷 설정
        current_time = datetime.now().astimezone(korea_tz)
        year_month_day = current_time.strftime("year=%Y/month=%m/day=%d")

        # S3 버킷 경로 설정
        s3_path = f'sql_tokenized/{year_month_day}/'

        # Parquet 파일로 저장
        parquet_file = f'{current_time.strftime("%H:%M:%S")}.parquet'
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file)

        # S3에 Parquet 파일 업로드
        s3.upload_file(parquet_file, 'chiholee-sql', f'{s3_path}{parquet_file}')

        # 로컬에 생성된 Parquet 파일 삭제
        os.remove(parquet_file)

    #####################################

    if full_text_list:
        # 데이터를 Parquet 형식으로 변환
        df = pd.DataFrame(full_text_list)

        # 날짜 및 시간 포맷 설정
        current_time = datetime.now().astimezone(korea_tz)
        year_month_day = current_time.strftime("year=%Y/month=%m/day=%d")

        # S3 버킷 경로 설정
        s3_path = f'sql_fulltext/{year_month_day}/'

        # Parquet 파일로 저장
        parquet_file = f'{current_time.strftime("%H:%M:%S")}.parquet'
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file)

        # S3에 Parquet 파일 업로드
        s3.upload_file(parquet_file, 'chiholee-sql', f'{s3_path}{parquet_file}')

        # 로컬에 생성된 Parquet 파일 삭제
        os.remove(parquet_file)