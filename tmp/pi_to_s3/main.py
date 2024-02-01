import boto3
import time
from datetime import datetime
from base64 import encode
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd 
import pytz 

region = 'ap-northeast-2'

pi_client = boto3.client('pi', region)
s3 = boto3.client('s3', region_name=region)
rds_client = boto3.client('rds', region)
cw_client = boto3.client('cloudwatch', region)

korea_tz = pytz.timezone('Asia/Seoul')

db_identifier='db-IUJELG26COMQKPV7RDTERN3WR4'

def sql_detail(groupidentifier) :
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
    StartTime=time.time() - (60*60),
    EndTime=time.time(),
    PeriodInSeconds=1
)

sql_list = []
full_text_list = []

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

        

        
        data = {
            "db_sql_id": db_sql_id,
            "db_sql_tokenized_id": db_sql_tokenized_id,
            "timestamp": timestamp.astimezone(korea_tz).strftime("%Y-%m-%d.%H:%M:%S"),
            "cpu_load": v
        }
        
        sql_list.append(data)

        data_detail = {
            "db_sql_id": db_sql_id,
            "sql_fulltxt": sql_detail(db_sql_id)
        }

        full_text_list.append(data_detail)

# 데이터를 Parquet 형식으로 변환
table = pa.Table.from_pandas(pd.DataFrame(sql_list))

# Parquet 파일로 저장
current_time = datetime.now().strftime("%Y%m%d.%H:%M:%S")
parquet_file = f'{current_time}.parquet'
pq.write_table(table, parquet_file)

# S3에 Parquet 파일 업로드
s3.upload_file(parquet_file, 'chiholee-sql', f'sql_tokenized/{parquet_file}')

# 로컬에 생성된 Parquet 파일 삭제
os.remove(parquet_file)

#####################################

# 데이터를 Parquet 형식으로 변환
table = pa.Table.from_pandas(pd.DataFrame(full_text_list))

# Parquet 파일로 저장
current_time = datetime.now().strftime("%Y%m%d.%H:%M:%S")
parquet_file = f'{current_time}.parquet'
pq.write_table(table, parquet_file)

# S3에 Parquet 파일 업로드
s3.upload_file(parquet_file, 'chiholee-sql', f'sql_fulltext/{parquet_file}')

# 로컬에 생성된 Parquet 파일 삭제
os.remove(parquet_file)




