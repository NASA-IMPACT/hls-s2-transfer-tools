import boto3
import json
import multiprocessing
import requests
stack_name = "hls-prod"
limit = 100
offset = 10480000
cf = boto3.client("cloudformation")

def get_resources(stackname, nextToken=None):
    if nextToken is None:
        resources = cf.list_stack_resources(StackName=stackname)
    else:
        resources = cf.list_stack_resources(StackName=stackname, NextToken=nextToken)
    return resources

def get_RDS(resources):
    RDS_status = False
    RDS_resources = {}
    for summary in resources["StackResourceSummaries"]:
        if "Rds" in summary["LogicalResourceId"]:
            RDS_status = True
            RDS_resources.update({f"{summary['ResourceType']}": summary["PhysicalResourceId"]})
    return RDS_status, RDS_resources

def submit_query(limit, offset):
    query = client.execute_statement(
        resourceArn=db_arn,
        secretArn=RDS_resources['AWS::SecretsManager::Secret'],
        sql=f'SELECT * FROM sentinel_log WHERE historic AND jobinfo is NULL limit {limit} offset {offset};',
        database='hls',
        )
    return query

def check_cmr(s2_scene, result_dict):
    url = f"https://cmr.earthdata.nasa.gov/search/granules.json?collection_concept_id=C2021957295-LPCLOUD&attribute[]=string,PRODUCT_URI,{s2_scene}.SAFE"
    resp = requests.get(url).json()
    hits = len(resp["feed"]["entry"])
    if hits < 1:
        result_dict[s2_scene] = "missing"
    else:
        result_dict[s2_scene] = resp["feed"]["entry"][0]["producer_granule_id"]

resources = get_resources(stack_name)
RDS_status, RDS_resources = get_RDS(resources)
while RDS_status == False and resources.get("NextToken") is not None:
    resources = get_resources(stack_name, resources.get("NextToken"))
    RDS_status, RDS_resources = get_RDS(resources)

rds = boto3.client("rds")

cluster_info = rds.describe_db_clusters(DBClusterIdentifier=RDS_resources['AWS::RDS::DBCluster'])
db_arn = cluster_info["DBClusters"][0]["DBClusterArn"]
client = boto3.client("rds-data")

query = submit_query(limit, offset)
manager = multiprocessing.Manager()
result_dict = manager.dict()
while len(query["records"]) > 0:
    s2_ids = []
    jobs = []
    for record in query["records"]:
        s2_ids.extend(record[3]['stringValue'].split(","))
    for s2_id in s2_ids:
        p = multiprocessing.Process(target=check_cmr, args=(s2_id, result_dict))
        jobs.append(p)
        p.start()
    for proc in jobs:
        proc.join()
    #cmr = pool.map_async(check_cmr,s2_ids,chunksize=1)
    #print(dir(cmr))
    offset += limit
    query = submit_query(limit,offset)
values = list(result_dict.values())
missing = values.count("missing")
found = sum(1 for x in values if "HLS.S30" in x)
print(missing)
print(found)

with open("test_output.json","w") as f:
    json.dump(dict(result_dict),f)

print("There are no more entries in the database")
