import requests

import boto3
import datetime
import gzip
import json
import os
import pandas as pd
import sys
import time


class query_inventory():

    def __init__(self):
        self.client = boto3.client("athena")
        with open("database_params.json", "r") as f:
            params = json.load(f)
        self.catalog = params["catalog"]
        self.database = params["database"]
        self.table = params["table"]
        self.output_location = params["output_location"]
        self.inventory_path = params["inventory_location"]
        self.inventory_path += "/" if not self.inventory_path.endswith("/") else self.inventory_path
        self.start_date = datetime.datetime.strptime(sys.argv[1], "%Y%j")
        self.missing = []
        print(datetime.datetime.now())
        self.check_table()
        self.query_manager()
        print(len(self.missing))
        count = 0
        for obj in self.missing:
            if obj.split("_")[5][3] == "X":
                count += 1
        print(f"files in X row of MGRS grid: {count}")
        print(datetime.datetime.now())

    def check_table(self):
        queryString = f"SHOW TABLES LIKE '{self.table}'"
        tables = self.query_athena(queryString)
        if len(tables["ResultSet"]["Rows"]) < 1:
            with open("table_params.txt","r") as f:
                queryString = f.read().replace("\n","")
            queryString = queryString.format(self.table,self.inventory_path)
            result = self.query_athena(queryString)
        else:
            print(" ".join([
                    f"Table {self.table} exists in {self.database}.",
                    "Retrieving the most recent partition"
                    ]
                   )
                 )

    def get_last_partition(self):
        queryString = f'SELECT * FROM "{self.table}$partitions" ORDER BY dt DESC'
        partitions = self.query_athena(queryString)
        if len(partitions["ResultSet"]["Rows"]) >= 2:
            partitionDate = partitions["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
        else:
            queryString = f'MSCK REPAIR TABLE {self.table}'
            resp = self.query_athena(queryString)
            self.get_last_partition()
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
            print(self.get_query_results(queryId))
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
        self.end_date = self.start_date + datetime.timedelta(days=1)
        start_date = f"{self.start_date:%Y-%m-%dT00:00:00}"
        end_date = f"{self.end_date:%Y-%m-%dT00:00:00}"
        queryString = " ".join([
                f"SELECT key, size, last_modified FROM {self.table}",
                f"WHERE dt='{self.partitionDate}' AND",
                "date_parse(last_modified, '%Y-%m-%dT%H:%i:%s.%fZ') >=",
                f"date_parse('{start_date}', '%Y-%m-%dT%H:%i:%s') AND",
                "date_parse(last_modified, '%Y-%m-%dT%H:%i:%s.%fZ') <",
                f"date_parse('{end_date}', '%Y-%m-%dT%H:%i:%s') ORDER BY",
                "date_parse(last_modified, '%Y-%m-%dT%H:%i:%s.%fZ')"
                ]
            )
        result = self.query_athena(queryString)
        if len(result["ResultSet"]["Rows"]) < 2:
            print("The query returned a response with 0 rows. Nothing more to do. Exiting.")
            exit()
        self.read_csv()

    def read_csv(self):
        path = self.output.split("//")[-1]
        self.bucket = path.split("/")[0]
        key = "/".join(path.split("/")[1:])
        inventory = pd.read_csv(self.output,
                                sep=",",
                                header=0,
                                index_col="key"
                                )
        #inventory = inventory.set_index("key")
        inventory.loc[:,"key"] = [x.split("/")[-1].strip(".zip") for x in inventory.index]
        print(len(inventory["key"]))
        for scene in inventory["key"]:
            self.check_cmr(scene)

    def check_cmr(self, s2_scene):
        url = f"https://cmr.earthdata.nasa.gov/search/granules.xml?collection_concept_id=C2021957295-LPCLOUD&attribute[]=string,PRODUCT_URI,{s2_scene}.SAFE"
        resp = requests.get(url).text
        hits = int(resp.split("<hits>")[1].split("</hits>")[0])
        if hits < 1:
            self.missing.append(s2_scene)

files = query_inventory()
