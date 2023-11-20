import boto3

ath = boto3.client("athena")

with open('table_params.txt') as f:
    resp = ath.start_query_execution(
            QueryString=f.read(),
            ResultConfiguration={'OutputLocation': 's3://impact-s2-inventories/queries'}
            )
