import boto3
import datetime
import gzip
import json
import os
import pandas as pd
import subprocess
import sys
import time

from multiprocessing import Pool

def pull_aws_config():
    homedir = os.environ["HOME"]
    with open(f"{homedir}/.aws/config") as f:
        config = f.read().split("\n")
        for att in config:
           if "source_profile" in att:
               profile = att.split("=")[1].strip()
           else:
               profile = "default"

    return profile

def check_table():
    with open("table_params.txt","r") as f:
        queryString = f.read()
    queryString = queryString.format(table)
    result = client.start_query_execution(
        QueryString=queryString,
        ResultConfiguration={"OutputLocation": output_location}
    )

def get_last_partition():
    try:
        queryString = f'SELECT * FROM "{table}$partitions" ORDER BY dt DESC'
        partitions,output = query_athena(queryString)
    except:
        queryString = f"MSCK REPAIR TABLE {table}"
        result, output = query_athena(queryString)
        queryString = f'SELECT * FROM "{table}$partitions" ORDER BY dt DESC'
        partitions, output = query_athena(queryString)
    if len(partitions["ResultSet"]["Rows"]) >= 2:
        partitionDate = partitions["ResultSet"]["Rows"][2]["Data"][0]["VarCharValue"]
    return partitionDate

def query_athena(queryString):
    query = submit_query(queryString)
    queryId = query["QueryExecutionId"]
    state, output = get_query_state(queryId)
    while state == "QUEUED" or state == "RUNNING":
        print(f"Query state is currently {state} for Query: '{queryString}'. Waiting 15 seconds")
        time.sleep(15)
        state, output = get_query_state(queryId)
    if state == "SUCCEEDED":
        result = get_query_results(queryId)
        return result, output

    elif state == "FAILED" or state == "CANCELLED":
        print(f"Query returned as {state}. Exiting.")
    else:
        print("You should not be here. Exiting.")
    exit()

def submit_query(queryString):
    response = client.start_query_execution(
            QueryString=queryString,
            QueryExecutionContext={
                "Catalog": catalog,
                "Database": database
                },
            ResultConfiguration={
                "OutputLocation": output_location
                }
            )
    return response

def get_query_state(queryId):
    response = client.get_query_execution(
            QueryExecutionId=queryId
            )
    output = response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    state = response["QueryExecution"]["Status"]["State"]
    if state == "FAILED":
        print(response)
    return state, output

def get_query_results(queryId):
    response = client.get_query_results(
            QueryExecutionId=queryId
            )
    return response

def query_manager():
    date = datetime.date.today()
    partitionDate = get_last_partition()
    partitionDate = datetime.datetime.strptime(partitionDate,"%Y-%m-%d-%H-%M").date()
    if date > partitionDate:
        queryString = f"MSCK REPAIR TABLE {table}"
        result = query_athena(queryString)
        partitionDate = get_last_partition()
    print(f"Successfully found partition for {partitionDate}")
    output = get_files(partitionDate)
    return output

def get_files(partitionDate):
    try:
        scene_filter = f"%/{year:04}/{month:02}/{day:02}/%"
    except:
        scene_filter = f"%/{year:04}/{month:02}/%"
    if date < s2b_start_date and date >= s2a_start_date:
        scene_filter += "S2B%"
    elif date < s2a_start_date:
        scene_filter += ""
    else:
        exit()
    queryString = " ".join([
            f"SELECT key FROM {table}",
            f"WHERE dt='{partitionDate}' AND",
            f"key like '{scene_filter}'"
            ]
        )
    result, output = query_athena(queryString)
    return output, scene_filter

def read_csv(output, scene_filter):
    path = output.split("//")[-1]
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])
    inventory = pd.read_csv(output,
                            names = ["key"],
                            sep=",",
                            skiprows=1,
                            header=None
                            #storage_options = {
                            #    "key": accessKeyId,
                            #    "secret": secretAccessKey,
                            #    "token": sessionToken,
                            # }
                            )
    keys = inventory["key"].tolist()
    nobj = len(keys)
    print(f"Successfully retrieved {nobj} objects with key filter {scene_filter}")
    return keys

def move_files(file):
    filename = file.split("/")[-1]
    file_comp = filename.split("_")
    date = datetime.datetime.strptime(file_comp[2],"%Y%m%dT%H%M%S")
    utm_info = file_comp[5]
    utm_zone = utm_info[1:3]
    lat_band = utm_info[3]
    square = utm_info[4:]
    new_key = f"{utm_zone}/{lat_band}/{square}/{date:%Y/%-m/%-d}/{filename}"
    source_file = f"s3://{source_bucket}/{file}"
    trigger_file = f"s3://{target_bucket}/{filename}"
    archive_file = f"s3://{archive_bucket}/{new_key}"
    subprocess.run(["aws", "s3", "rm", source_file, "--quiet"])

profile = pull_aws_config()
session = boto3.session.Session(profile_name=profile)
client = session.client("athena")
s3Client = session.client("s3")
accessKeyId = session.get_credentials().access_key
secretAccessKey = session.get_credentials().secret_key
sessionToken = session.get_credentials().token
date = datetime.datetime.strptime(sys.argv[1],"%Y%j")
year = date.year
month = date.month
day = date.day
with open("database_params.json", "r") as f:
    params = json.load(f)
catalog = params["catalog"]
database = params["database"]
table = params["table"]
output_location = params["output_location"]
local_path = params.get("local_path", False)
source_bucket = params["source_bucket"]
target_bucket = params["target_bucket"]
archive_bucket = params["archive_bucket"]
s2b_start_date = datetime.datetime.strptime(params["s2b_start_date"],"%m-%d-%Y")
s2a_start_date = datetime.datetime.strptime(params["s2a_start_date"],"%m-%d-%Y")
check_table() #- need to figure out why query won't work for creating new table
output, scene_filter = query_manager()
print(output)
keys = read_csv(output, scene_filter) #read query output csv

print(datetime.datetime.now())
pool = Pool(40)
move = pool.map_async(move_files,keys,chunksize=1)
while not move.ready():
    print(f"Time: {datetime.datetime.now()} - {move._number_left} granules remaining")
    time.sleep(60)
print(datetime.datetime.now())
