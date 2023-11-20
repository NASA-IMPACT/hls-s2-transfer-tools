import boto3


client = boto3.client("s3")

filelist = client.list_objects(Bucket="esa-data-transfer-staging")
for obj in filelist["Contents"]:
    print(obj["Key"])
