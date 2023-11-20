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

class query_inventory():

    def __init__(self):
        profile = self.pull_aws_config()
        session = boto3.session.Session(profile_name=profile)
        self.client = session.client("athena")
        self.s3Client = session.client("s3")
        self.accessKeyId = session.get_credentials().access_key
        self.secretAccessKey = session.get_credentials().secret_key
        self.sessionToken = session.get_credentials().token
        date = datetime.datetime.strptime(sys.argv[1],"%Y%j")
        self.year = date.year
        self.month = date.month
        self.day = date.day
        with open("database_params.json", "r") as f:
            params = json.load(f)
        self.catalog = params["catalog"]
        self.database = params["database"]
        self.table = params["table"]
        self.output_location = params["output_location"]
        self.local_path = params.get("local_path", False)
        self.source_bucket = params["source_bucket"]
        self.target_bucket = params["target_bucket"]
        self.archive_bucket = params["archive_bucket"]
        self.check_table() #- need to figure out why query won't work for creating new table
        self.query_manager()
        self.read_csv() #read query output csv

    def pull_aws_config(self):
        homedir = os.environ["HOME"]
        with open(f"{homedir}/.aws/config") as f:
            config = f.read().split("\n")
            for att in config:
               if "source_profile" in att:
                   profile = att.split("=")[1].strip()
               else:
                   profile = "default"

        return profile

    def check_table(self):
        with open("table_params.txt","r") as f:
            queryString = f.read()
        queryString = queryString.format(self.table)
        result = self.client.start_query_execution(
            QueryString=queryString,
            ResultConfiguration={"OutputLocation": self.output_location}
        )

    def get_last_partition(self):
        try:
            queryString = f'SELECT * FROM "{self.table}$partitions" ORDER BY dt DESC'
            partitions = self.query_athena(queryString)
        except:
            queryString = f"MSCK REPAIR TABLE {self.table}"
            result = self.query_athena(queryString)
            queryString = f'SELECT * FROM "{self.table}$partitions" ORDER BY dt DESC'
            partitions = self.query_athena(queryString)
        if len(partitions["ResultSet"]["Rows"]) >= 2:
            partitionDate = partitions["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
        return partitionDate

    def query_athena(self, queryString):
        query = self.submit_query(queryString)
        queryId = query["QueryExecutionId"]
        state = self.get_query_state(queryId)
        while state == "QUEUED" or state == "RUNNING":
            print(f"Query state is currently {state} for Query: '{queryString}'. Waiting 15 seconds")
            time.sleep(15)
            state = self.get_query_state(queryId)

        if state == "SUCCEEDED":
            result = self.get_query_results(queryId)
            return result

        elif state == "FAILED" or state == "CANCELLED":
            print(f"Query returned as {state}. Exiting.")
        else:
            print("You should not be here. Exiting.")
        exit()

    def submit_query(self, queryString):
        response = self.client.start_query_execution(
                QueryString=queryString,
                QueryExecutionContext={
                    "Catalog": self.catalog,
                    "Database": self.database
                    },
                ResultConfiguration={
                    "OutputLocation": self.output_location
                    }
                )
        return response

    def get_query_state(self, queryId):
        response = self.client.get_query_execution(
                QueryExecutionId=queryId
                )
        self.output = response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        state = response["QueryExecution"]["Status"]["State"]
        if state == "FAILED":
            print(response)
        return state

    def get_query_results(self, queryId):
        response = self.client.get_query_results(
                QueryExecutionId=queryId
                )
        return response

    def query_manager(self):
        self.date = datetime.date.today()
        self.partitionDate = self.get_last_partition()
        partitionDate = datetime.datetime.strptime(self.partitionDate,"%Y-%m-%d-%H-%M").date()
        if self.date > partitionDate:
            queryString = f"MSCK REPAIR TABLE {self.table}"
            result = self.query_athena(queryString)
            self.partitionDate = self.get_last_partition()
        print(f"Successfully found partition for {self.partitionDate}")
        self.get_files()

    def get_files(self):
        try:
            self.scene_filter = f"%/{self.year:04}/{self.month:02}/{self.day:02}/%"
        except:
            self.scene_filter = f"%/{self.year:04}/{self.month:02}/%"
        queryString = " ".join([
                f"SELECT key FROM {self.table}",
                f"WHERE dt='{self.partitionDate}' AND",
                f"key like '{self.scene_filter}'"
                ]
            )
        result = self.query_athena(queryString)

    def read_csv(self):
        path = self.output.split("//")[-1]
        self.bucket = path.split("/")[0]
        key = "/".join(path.split("/")[1:])
        inventory = pd.read_csv(self.output,
                                names = ["key"],
                                sep=",",
                                skiprows=1,
                                header=None
                                #storage_options = {
                                #    "key": self.accessKeyId,
                                #    "secret": self.secretAccessKey,
                                #    "token": self.sessionToken,
                                # }
                                )
        keys = inventory["key"].tolist()
        nobj = len(keys)
        print(f"Successfully retrieved {nobj} objects with key filter {self.scene_filter.strip('%')}")
        def move_files(file):
            print(file)
            filename = file.split("/")[-1]
            file_comp = filename.split("_")
            date = datetime.datetime.strptime(file_comp[2],"%Y%m%dT%H%M%S")
            utm_info = file_comp[5]
            utm_zone = utm_info[1:3]
            lat_band = utm_info[3]
            square = utm_info[4:]
            new_key = f"{utm_zone}/{lat_band}/{square}/{date:%Y/%-m/%-d}/{filename}"
            source_file = f"s3://{self.source_bucket}/{file}"
            trigger_file = f"s3://{self.target_bucket}/{filename}"
            archive_file = f"s3://{self.archive_bucket}/{new_key}"
            subprocess.run(["aws", "s3", "cp", source_file, trigger_file, "--quiet"])
            subprocess.run(["aws", "s3", "mv", source_file, archive_file, "--quiet"])

        print(datetime.datetime.now())
        for file in keys:
            print(file)
            move_files(file)
        print(datetime.datetime.now())

if __name__ == "__main__":
    query_inventory()
