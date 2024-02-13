import boto3
import mysql.connector
import json
from pprint import pprint
import pytz 
from datetime import datetime
import urllib.parse

region = 'ap-northeast-2'
s3 = boto3.client('s3', region_name=region)

korea_tz = pytz.timezone('Asia/Seoul')

asis_db = {
    'alias':'asis_db',
    'host':'aurora-mysql-5-7-instance-1.cgkgybnzurln.ap-northeast-2.rds.amazonaws.com',
    'database':'ecommerce',
    'user':'admin',
    'password':'admin1234'
}

tobe_db = {
    'alias':'tobe_db',
    'host':'ecommerce-instance-1.cgkgybnzurln.ap-northeast-2.rds.amazonaws.com',
    'database':'ecommerce',
    'user':'admin',
    'password':'admin1234'
}

def find_and_print_table(json_obj, result=None):
    if result is None:
        result = []

    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if key == 'table':                
                table_name = value['table_name']
                access_type = value['access_type']
                index_name = value.get('key', "Full Scan")

                result.append(f"{table_name} / {access_type} / {index_name}")
                find_and_print_table(value, result)

            else:
                find_and_print_table(value, result)
    elif isinstance(json_obj, list):
        for item in json_obj:
            find_and_print_table(item, result)

    # pprint(result)
    return result

def execute_plan(db, sql) :

    # MySQL 연결 정보 설정
    config = {
        'user': db['user'],
        'password': db['password'],
        'host': db['host'],
        'database': db['database']
    }


    # MySQL에 연결
    connection = mysql.connector.connect(**config)

    try:
        # SQL 쿼리
        sql_query = sql

        # MySQL 커서 생성
        cursor = connection.cursor()

        # SQL 쿼리 실행
        cursor.execute("EXPLAIN FORMAT=JSON " + sql_query)

        # 실행 계획 가져오기
        explain_result = cursor.fetchone()

        # JSON 포맷의 실행 계획 출력
        # print(explain_result[0])

        explain_json = json.loads(explain_result[0])

        # pprint(explain_json)
        # print("####################")

        return find_and_print_table(explain_json), None

    except mysql.connector.Error as err:
        error = str(err)
        return None, error

    finally:
        # 연결 및 커서 닫기
        cursor.close()
        connection.close()

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    response = s3.get_object(Bucket=bucket, Key=key)
        
    # 파일 내용을 읽고 JSON 데이터로 파싱합니다.
    text = response["Body"].read().decode()
    data = json.loads(text)

    db_sql_tokenized_id = data['db_sql_tokenized_id']
    db_sql_id = data['db_sql_id']
    db_identifier = data['db_identifier']
    db_cluster_name = data['db_cluster_name']
    db_instance_name = data['db_instance_name']
    sql_type = data['sql_type']
    sql_fulltext = data['sql_fulltext']
    last_update_time = data['last_update_time']
    cpu_load = data['cpu_load']


    
    if sql_type != 'OTHER' :
        for db in [asis_db, tobe_db] :
            plan_result, error = execute_plan(db, sql_fulltext)

            print(plan_result)

            data_list = []
            ind = 1
            if plan_result :
                for plan in plan_result :
                    data = {
                        "id": ind,
                        "target_db": db['alias'],
                        "db_sql_tokenized_id": db_sql_tokenized_id,
                        "db_sql_id": db_sql_id,
                        "db_identifier": db_identifier,
                        "db_cluster_name": db_cluster_name,
                        "db_instance_name": db_instance_name,
                        "sql_type": sql_type,
                        "sql_plan": plan,
                        "sql_fulltext": sql_fulltext,
                        "last_update_time": datetime.now().astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    data_list.append(data)
                    ind += 1
            else :
                data = {
                    "id": ind,
                    "target_db": db['alias'],
                    "db_sql_tokenized_id": db_sql_tokenized_id,
                    "db_sql_id": db_sql_id,
                    "db_identifier": db_identifier,
                    "db_cluster_name": db_cluster_name,
                    "db_instance_name": db_instance_name,
                    "sql_type": sql_type,
                    "sql_plan": "",
                    "sql_fulltext": sql_fulltext,
                    "last_update_time": datetime.now().astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S"),
                    "error": error
                }
                
                data_list.append(data)
                ind += 1

            json_data = '\n'.join(json.dumps(item) for item in data_list)

            # 날짜 및 시간 포맷 설정
            current_time = datetime.now().astimezone(korea_tz)
            year_month_day = current_time.strftime("year=%Y/month=%m/day=%d")
            # S3 버킷 경로 설정
            s3_path = f'sql_plan/{year_month_day}'
            file_name = f"{db['alias']}_{db_sql_tokenized_id}.json"
            # S3에 파일 업로드
            s3.put_object(Bucket='chiholee-sql', Key=f"{s3_path}/{file_name}", Body=json_data)
                        


test_event = {
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-west-2",
      "eventTime": "2021-01-01T12:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AAAABBBBCCCCDDDD"
      },
      "requestParameters": {
        "sourceIPAddress": "123.123.123.123"
      },
      "responseElements": {
        "x-amz-request-id": "ExampleRequestId",
        "x-amz-id-2": "ExampleId2"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "ExampleConfig",
        "bucket": {
          "name": "chiholee-sql",
          "ownerIdentity": {
            "principalId": "A3NL1KOZZKExample"
          },
          "arn": "arn:aws:s3:::example-bucket"
        },
        "object": {
          "key": "sql_fulltext/year=2024/month=02/day=12/91BB112A23E91EE9753B7B78714748574662B421.json",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}

# context 객체는 이 예제에서는 사용하지 않지만, 필요에 따라 모방할 수 있습니다.
test_context = {}

if __name__ == "__main__":
    # 테스트 event와 context를 lambda_handler에 전달
    response = lambda_handler(test_event, test_context)