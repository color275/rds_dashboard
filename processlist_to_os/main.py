#! /home/ec2-user/resource_dashboard/rds_dashboard/processlist_to_os/venv/bin/python3

import boto3
from mysql.connector import connect
from datetime import datetime, timedelta
from pprint import pprint
import pytz
import time
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

region = 'ap-northeast-2'
rds_client = boto3.client('rds', region)

host = 'vpc-test-aponilxfo5qn2nfe6mitxf2rxu.ap-northeast-2.es.amazonaws.com'
service = 'es'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)
korea_timezone = pytz.timezone('Asia/Seoul')

es_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)

def get_rds_instances_with_tag(tag_key, tag_value):
    response = rds_client.describe_db_instances()
    
    target_instance = []
    
    for instance in response['DBInstances']:
        if instance.get('PerformanceInsightsEnabled') :
            for tag in instance.get('TagList', []):
                if tag.get('Key') == 'pi_monitor' and tag.get('Value') == 'true':
                    target_instance.append(instance)
                    break
    return target_instance


def fetch_mysql_processlist(instance,db_usernm,db_password):
    
    host = instance['Endpoint']['Address']

    cluster_name = instance['DBClusterIdentifier']
    instance_name = instance['DBInstanceIdentifier']

    mysql_config = {
        'host': host,
        'user': db_usernm,
        'password': db_password
    }
    connection = connect(**mysql_config)
    cursor = connection.cursor()
    cursor.execute("SHOW FULL PROCESSLIST")
    processlist = cursor.fetchall()
    cursor.close()
    connection.close()

    doc_list = []
    
    for process in processlist:
        
        filter_commands = ['Sleep', 'init', 'Daemon', 'Binlog Dump']
        filter_infos = ['SHOW FULL PROCESSLIST', 'SHOW PROCESSLIST']

        # filter_commands = ['Sleep']
        # filter_infos = []
        # filter_state = ['User sleep']
        # print(process[7])
        if process[4] not in filter_commands :
           if process[7] not in filter_infos :
            # if process[6] == 'User sleep' :
                doc = {
                    'cluster' : cluster_name,
                    'instance' : instance_name,
                    'id': process[0],
                    'user': process[1],
                    'host': process[2],
                    'db': process[3],
                    'command': process[4],
                    'time': process[5],
                    'state': process[6],
                    'info': process[7],
                    'view_yn': 1,
                    'timestamp': datetime.now(korea_timezone),
                }
                doc_list.append(doc)                
    return doc_list

def index_delete_data_opensearch(os_index_nm):
    # 인덱스의 모든 문서 삭제
    delete_query = {
        "query": {
            "match_all": {}
        }
    }
    es_client.delete_by_query(index=os_index_nm, 
                              body=delete_query,
                              conflicts="proceed")
    
#     # 새로운 데이터 삽입
#     for doc in doc_list:
#         es_client.index(index=os_index_nm, body=doc)
#         pprint(doc)

def index_processlist_to_opensearch(doc_list, os_index_nm, insert_doc_id): 
    
    
    if not es_client.indices.exists(index=os_index_nm):
        es_client.indices.create(index=os_index_nm)

    for doc in doc_list:
        # 문서 식별을 위한 고유 ID 추출 (예: 'id' 필드 사용)
        # 'id' 필드가 문서의 고유 식별자인 경우
        cluster = doc.get('cluster')
        processlist_id = doc.get('id')
        instance = doc.get('instance')
        
        # cluster, id, instance 필드가 모두 존재하는 경우에만 업데이트 또는 삽입 진행
        if cluster is not None and processlist_id is not None and instance is not None:
            # 검색 쿼리를 구성하여 조건에 맞는 문서를 찾습니다.
            search_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"cluster": cluster}},
                            {"match": {"id": processlist_id}},
                            {"match": {"instance": instance}}
                        ]
                    }
                }
            }
            search_resp = es_client.search(index=os_index_nm, body=search_query)
            hits = search_resp['hits']['hits']

            # 검색된 문서가 있으면, 첫 번째 검색 결과의 _id를 사용하여 문서를 업데이트합니다.
            if hits:
                existing_doc_id = hits[0]['_id']
                update_resp = es_client.update(index=os_index_nm, id=existing_doc_id, body={"doc": doc})

                insert_doc_id.append(existing_doc_id)                
            else:
                # 조건에 맞는 문서가 없으면, 새로운 문서로 삽입합니다.
                # 여기서는 _id를 명시적으로 지정하지 않았기 때문에 OpenSearch가 자동으로 생성합니다.
                insert_resp = es_client.index(index=os_index_nm, body=doc)
                insert_doc_id.append(insert_resp['_id'])
        else:
            print("Document is missing one or more required fields (cluster, id, instance):", doc)
    
    return insert_doc_id

        
def delete_old_documents(os_index_nm, sec):
    # 현재 시간에서 sec초 전의 시간을 계산합니다.
    ten_seconds_ago = datetime.now(korea_timezone) - timedelta(seconds=sec)    
    # sec초 전의 시간보다 이전에 생성된 문서를 검색하기 위한 쿼리를 작성합니다.
    search_query = {
        "query": {
            "range": {
                "timestamp": {
                    "lte": ten_seconds_ago
                }
            }
        }
    }

    # 검색 쿼리를 사용하여 인덱스에서 sec초 이상이 지난 모든 문서를 검색합니다.
    search_resp = es_client.search(index=os_index_nm, body=search_query)
    
    hits = search_resp['hits']['hits']
    num_deleted = 0

    # 검색된 각 문서를 삭제합니다.
    for hit in hits:
        doc_id = hit['_id']        
        delete_resp = es_client.delete(index=os_index_nm, id=doc_id)
        num_deleted += 1

    # print(f"Deleted {num_deleted} documents older than {sec} seconds.")

if __name__ == '__main__':
    tag_key = 'pi-monitor'
    tag_value = 'true'
    os_index_nm = 'rds-monitor-showprocesslist-2'
    db_usernm = 'admin'
    db_password = 'admin1234'
    instances = get_rds_instances_with_tag(tag_key, tag_value)
    insert_doc_id = []
    # print(instances)
    
    # index_delete_data_opensearch(os_index_nm)
    
    for instance in instances:
        doc_list = fetch_mysql_processlist(instance,db_usernm,db_password)
        index_processlist_to_opensearch(doc_list, os_index_nm, insert_doc_id)
    
    
    # print(insert_doc_id)
    time.sleep(1)
    delete_old_documents(os_index_nm, 5)
    