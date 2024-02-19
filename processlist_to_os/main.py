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
    cursor = connection.cursor(dictionary=True)


    sql = """
select /* sqlid : admin */
	esc.digest as digest,
    p.id as processlist_id,
    p.user as user,
    p.host as host,
    p.db as db,
    p.command as command,
    p.time as time,
    p.state as state,
    p.info as info,    
    esc.digest_text as digest_text
from 
    performance_schema.threads as t
join 
    information_schema.processlist as p on t.processlist_id = p.id
left join 
    performance_schema.events_statements_current as esc on t.thread_id = esc.thread_id
where 
    t.type = 'foreground' and 
    p.command != 'sleep' and
    p.info is not null
order by 
    p.time desc, p.id
    """
    
    cursor.execute(sql)
    processlist = cursor.fetchall()
    cursor.close()
    connection.close()

    doc_list = []
    
    for process in processlist:
        
        if 'select /* sqlid : admin */' in process['info'] :
            continue

        doc = {
            'cluster': cluster_name,
            'instance': instance_name,
            'id': process['processlist_id'],
            'user': process['user'],
            'host': process['host'],
            'db': process['db'],
            'command': process['command'],
            'time': process['time'],
            'state': process['state'],
            'info': process['info'],
            'digest': process['digest'],
            'digest_text': process['digest_text'],
            'timestamp': datetime.now(korea_timezone),
        }
        doc_list.append(doc)                    
    return doc_list

def index_delete_data_opensearch(os_index_nm):
    delete_query = {
        "query": {
            "match_all": {}
        }
    }
    es_client.delete_by_query(index=os_index_nm, 
                              body=delete_query,
                              conflicts="proceed")
    

def index_processlist_to_opensearch(doc_list, os_index_nm, insert_doc_id): 
    
    
    if not es_client.indices.exists(index=os_index_nm):
        es_client.indices.create(index=os_index_nm)

    for doc in doc_list:
        cluster = doc.get('cluster')
        processlist_id = doc.get('id')
        instance = doc.get('instance')
        
        if cluster is not None and processlist_id is not None and instance is not None:
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

            if hits:
                existing_doc_id = hits[0]['_id']
                update_resp = es_client.update(index=os_index_nm, id=existing_doc_id, body={"doc": doc})

                insert_doc_id.append(existing_doc_id)                
            else:
                insert_resp = es_client.index(index=os_index_nm, body=doc)
                insert_doc_id.append(insert_resp['_id'])
        else:
            print("Document is missing one or more required fields (cluster, id, instance):", doc)
    
    return insert_doc_id

        
def delete_old_documents(os_index_nm, sec):
    ten_seconds_ago = datetime.now(korea_timezone) - timedelta(seconds=sec)    
    search_query = {
        "query": {
            "range": {
                "timestamp": {
                    "lte": ten_seconds_ago
                }
            }
        }
    }

    search_resp = es_client.search(index=os_index_nm, body=search_query)
    
    hits = search_resp['hits']['hits']
    num_deleted = 0

    for hit in hits:
        doc_id = hit['_id']        
        delete_resp = es_client.delete(index=os_index_nm, id=doc_id)
        num_deleted += 1


if __name__ == '__main__':
    tag_key = 'pi-monitor'
    tag_value = 'true'
    os_index_nm = 'rds-monitor-showprocesslist-3'
    db_usernm = 'admin'
    db_password = 'admin1234'
    instances = get_rds_instances_with_tag(tag_key, tag_value)
    insert_doc_id = []
    
    for instance in instances:
        doc_list = fetch_mysql_processlist(instance,db_usernm,db_password)
        index_processlist_to_opensearch(doc_list, os_index_nm, insert_doc_id)
    
    time.sleep(1)
    delete_old_documents(os_index_nm, 5)
    