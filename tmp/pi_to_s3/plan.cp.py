import boto3
import mysql.connector
import json
from pprint import pprint
import time
import pytz 
from datetime import datetime

region = 'ap-northeast-2'
athena = boto3.client('athena', region_name=region)
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

            else:
                find_and_print_table(value, result)
    elif isinstance(json_obj, list):
        for item in json_obj:
            find_and_print_table(item, result)

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

        return find_and_print_table(explain_json), None

    except mysql.connector.Error as err:
        error = str(err)
        return None, error

    finally:
        # 연결 및 커서 닫기
        cursor.close()
        connection.close()

def main():
    # SQL 쿼리 실행
    query = """
    SELECT db_sql_tokenized_id, db_sql_id, db_identifier, db_instance_name, sql_type, sql_fulltext
    FROM datalake01.sql_fulltext
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
        result = athena.get_query_results(QueryExecutionId=query_execution_id)
        # 결과 데이터를 딕셔너리로 변환
        # 컬럼 정보 추출
        columns = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        
        # 데이터 추출
        sql_list = []
        for row in result['ResultSet']['Rows'][1:]:  # 첫 번째 행은 컬럼 이름이므로 제외
            # 각 행의 데이터를 딕셔너리 형태로 변환
            row_data = {columns[i]: value.get('VarCharValue', '') for i, value in enumerate(row['Data'])}
            sql_list.append(row_data)
        
        for sql in sql_list :
            if sql['sql_type'] != 'OTHER' :
                for db in [asis_db, tobe_db] :
                    print(sql['sql_fulltext'])
                    plan_result, error = execute_plan(db, sql['sql_fulltext'])

                    data_list = []
                    ind = 1
                    if plan_result :
                        for plan in plan_result :
                            data = {
                                "id": ind,
                                "target_db": db['alias'],
                                "db_sql_tokenized_id": sql['db_sql_tokenized_id'],
                                "db_sql_id": sql['db_sql_id'],
                                "db_identifier": sql['db_identifier'],
                                "db_cluster_name": sql['db_cluster_name'],
                                "db_instance_name": sql['db_instance_name'],
                                "sql_type": sql['sql_type'],
                                "sql_plan": plan,
                                "sql_fulltext": sql['sql_fulltext'],
                                "last_update_time": datetime.now().astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S")
                            }
                            
                            data_list.append(data)
                            ind += 1
                    else :
                        data = {
                            "id": ind,
                            "target_db": db['alias'],
                            "db_sql_tokenized_id": sql['db_sql_tokenized_id'],
                            "db_sql_id": sql['db_sql_id'],
                            "db_identifier": sql['db_identifier'],
                            "db_cluster_name": sql['db_cluster_name'],
                            "db_instance_name": sql['db_instance_name'],
                            "sql_type": sql['sql_type'],
                            "sql_plan": "",
                            "sql_fulltext": sql['sql_fulltext'],
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
                    file_name = f"{db['alias']}-{sql['db_sql_tokenized_id']}.json"
                    # S3에 파일 업로드
                    s3.put_object(Bucket='chiholee-sql', Key=f"{s3_path}/{file_name}", Body=json_data)
                        


if __name__ == "__main__":
	main()




    #     # 결과 가져오기
    #     result = athena.get_query_results(QueryExecutionId=query_execution_id)

    #     first_row = True
    #     for row in result['ResultSet']['Rows']:        
    #         if first_row :
    #             first_row = False            
    #             continue
            
    #         print("## " + row['Data'][1]['VarCharValue'])
    #         sql = row['Data'][3]['VarCharValue']
    #         # print(sql)        
    #         for host in db_hosts :
    #             execute_plan(host, sql)
    #             print("")
    # else:
    #     print(f"Query failed with status '{status}'")