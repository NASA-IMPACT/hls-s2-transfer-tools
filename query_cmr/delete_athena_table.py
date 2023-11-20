import boto3
import sys

table = sys.argv[1]
ath = boto3.client("athena")

ath.start_query_execution(
    QueryString=f"DROP TABLE `{table}`",
    ResultConfiguration={'OutputLocation': 's3://impact-s2-inventories/queries'}
    )
