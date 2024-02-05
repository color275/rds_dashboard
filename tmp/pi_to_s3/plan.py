import boto3
import mysql.connector
import json
from pprint import pprint
import time

region = 'ap-northeast-2'
athena = boto3.client('athena', region_name=region)

db_hosts = [
    'ecommerce-instance-1.cgkgybnzurln.ap-northeast-2.rds.amazonaws.com',
    'aurora-mysql-5-7-instance-1.cgkgybnzurln.ap-northeast-2.rds.amazonaws.com',
]



def find_and_print_table(json_obj):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if key == 'table':                
                table_name = value['table_name']
                access_type = value['access_type']
                index_name = value['key'] if value.get('key') else "Full Scan"
                print(f"{table_name} / {access_type} / {index_name}")

            else:
                find_and_print_table(value)
    elif isinstance(json_obj, list):
        for item in json_obj:
            find_and_print_table(item)

def execute_plan(db_host, sql) :

    # MySQL 연결 정보 설정
    config = {
        'user': 'admin',
        'password': 'admin1234',
        'host': db_host,
        'database': 'ecommerce'
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

        find_and_print_table(explain_json)

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # 연결 및 커서 닫기
        cursor.close()
        connection.close()


# SQL 쿼리 실행
query = """
select b.last_update_time, a.db_sql_tokenized_id, b.db_sql_id, b.sql_fulltext
from datalake01.sql_tokenized_unique a,
     datalake01.sql_fulltext b
where a.db_sql_id = b.db_sql_id
order by b.last_update_time desc
limit 1
"""
query_start = athena.start_query_execution(
    QueryString=query,
    ResultConfiguration={
        'OutputLocation': 's3://chiholee-athena/',
    }
)

# 쿼리 실행 ID
query_execution_id = query_start['QueryExecutionId']

# 쿼리 실행 상태 확인
status = 'RUNNING'
while status in ['RUNNING', 'QUEUED']:
    time.sleep(5)  # 상태 확인 사이에 대기
    query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
    status = query_status['QueryExecution']['Status']['State']



if status == 'SUCCEEDED':
    # 결과 가져오기
    result = athena.get_query_results(QueryExecutionId=query_execution_id)

    first_row = True
    for row in result['ResultSet']['Rows']:        
        if first_row :
            first_row = False            
            continue
        
        print("## " + row['Data'][1]['VarCharValue'])
        sql = row['Data'][3]['VarCharValue']
        # print(sql)        
        for host in db_hosts :
            execute_plan(host, sql)
            print("")
else:
    print(f"Query failed with status '{status}'")