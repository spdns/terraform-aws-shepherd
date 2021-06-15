import boto3
import re
import time
import botocore
import sys
from awsglue.utils import getResolvedOptions

VALID_DATA_TYPES = ["dns", "proxy"]
PROXY_SUFFIX = ".proxy/"

args = getResolvedOptions(
    sys.argv,
    [
        "region",
        "database",
        "tableName",
        "athenaResultBucket",
        "athenaResultFolder",
        "athenaWorkgroup",
        "s3Bucket",
        "s3Folder",
        "dataType",
    ],
)
params = {
    "region": args["region"],
    "database": args["database"],
    "tableName": args["tableName"],
    "athenaResultBucket": args["athenaResultBucket"],
    "athenaResultFolder": args["athenaResultFolder"],
    "athenaWorkgroup": args["athenaWorkgroup"],
    "s3Bucket": args["s3Bucket"],
    "s3Folder": args["s3Folder"],
    "dataType": args["dataType"],
    "timeout": int(30),  # in sec
}


class ConfigurationException(Exception):
    pass


print("Parameters : ")
print(params)
if not params.get("dataType", "").lower() in VALID_DATA_TYPES:
    raise ConfigurationException(
        "Invalid dataType param received: %s. Valid params: %s"
        % (args.get("dataType"), VALID_DATA_TYPES)
    )
print("----------------------------------")
print()
s3Client = boto3.client("s3", region_name=params["region"])
s3Resource = boto3.resource("s3")
athenaClient = boto3.client("athena", region_name=params["region"])


def s3CheckIfBucketExists(s3Resource, bucketName):
    try:
        s3Resource.meta.client.head_bucket(Bucket=bucketName)
        print("Athena Bucket exists")
        print("----------------------------------")
        print()
    except botocore.exceptions.ClientError as e:
        print("Athena Bucket does not exist.")
        print(e)
        print("----------------------------------")
        location = {"LocationConstraint": params["region"]}
        s3Client.create_bucket(
            Bucket=params["s3Bucket"], CreateBucketConfiguration=location
        )
        print()
        print("Athena Bucket Created Successfully.")
        print()


def athena_query(athenaClient, queryString):
    response = athenaClient.start_query_execution(
        QueryString=queryString,
        QueryExecutionContext={"Database": params["database"]},
        ResultConfiguration={
            "OutputLocation": "s3://"
            + params["athenaResultBucket"]
            + "/"
            + params["athenaResultFolder"]
            + "/"
        },
        WorkGroup=params["athenaWorkgroup"],
    )
    return response


def athena_to_s3(athenaClient, params):
    queryString = "SHOW PARTITIONS " + params["tableName"]
    print("Show Partition Query : ")
    print(queryString)
    print("----------------------------------")
    print()
    execution = athena_query(athenaClient, queryString)
    execution_id = execution["QueryExecutionId"]
    state = "RUNNING"
    while state in ["RUNNING", "QUEUED"]:
        response = athenaClient.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in response
            and "Status" in response["QueryExecution"]
            and "State" in response["QueryExecution"]["Status"]
        ):
            state = response["QueryExecution"]["Status"]["State"]
            if state == "FAILED":
                print(response)
                print("state == FAILED")
                return False
            elif state == "SUCCEEDED":
                s3_path = response["QueryExecution"]["ResultConfiguration"][
                    "OutputLocation"
                ]
                filename = re.findall(r".*\/(.*)", s3_path)[0]
                return filename
        time.sleep(1)
    return False


def s3ListObject(s3, prefix):
    resultList = []
    result = s3.list_objects_v2(
        Bucket=params["s3Bucket"],
        Delimiter="/",
        Prefix=prefix,
        RequestPayer="requester",
    )
    resultList.extend(result.get("CommonPrefixes"))
    while result["IsTruncated"]:
        result = s3.list_objects_v2(
            Bucket=params["s3Bucket"],
            Delimiter="/",
            Prefix=prefix,
            RequestPayer="requester",
            ContinuationToken=result["NextContinuationToken"],
        )
        resultList.extend(result.get("CommonPrefixes"))
    return resultList


def s3ListRootObject(s3):
    resultList = []
    result = s3.list_objects_v2(
        Bucket=params["s3Bucket"], Delimiter="/", RequestPayer="requester"
    )
    commonPrefixes = result.get("CommonPrefixes")
    if commonPrefixes is not None:
        resultList.extend(commonPrefixes)
    while result["IsTruncated"]:
        result = s3.list_objects_v2(
            Bucket=params["s3Bucket"],
            RequestPayer="requester",
            Delimiter="/",
            ContinuationToken=result["NextContinuationToken"],
        )
        commonPrefixes = result.get("CommonPrefixes")
        if commonPrefixes is not None:
            resultList.extend(commonPrefixes)
    return resultList


def cleanup(s3Resource, params):
    print("Cleaning Temp Folder Created: ")
    print(params["athenaResultBucket"] + "/" + params["athenaResultFolder"] + "/")
    print()
    s3Resource.Bucket(params["athenaResultBucket"]).objects.filter(
        Prefix=params["athenaResultFolder"]
    ).delete()
    print("Cleaning Completed")
    print("----------------------------------")
    print()
    # s3Resource.Bucket(params['athenaResultBucket']).delete()


# Check if Bucket Exists
s3CheckIfBucketExists(s3Resource, params["athenaResultBucket"])


# Fetch Athena result file from S3
s3_filename = athena_to_s3(athenaClient, params)
if isinstance(s3_filename, bool):
    print("Unable to connect to Athena")
    print("----------------------------------")
    print()
    sys.exit(1)
print("Athena Result File At :")
print(
    params["athenaResultBucket"]
    + "/"
    + params["athenaResultFolder"]
    + "/"
    + s3_filename
)
print("----------------------------------")
print()


# Read Athena Query Result file and create a list of partitions present in athena meta
fileObj = s3Client.get_object(
    Bucket=params["athenaResultBucket"],
    Key=params["athenaResultFolder"] + "/" + s3_filename,
)
fileData = fileObj["Body"].read()
contents = fileData.decode("utf-8")
athenaList = contents.splitlines()
print("Athena Partition List : ")
print(athenaList)
print("----------------------------------")
print()


# Parse S3 folder structure and create partition list
prefix = params["s3Folder"]
subscriberFolders = s3ListRootObject(s3Client)

# Get only subscribers that either
# 1) Do NOT end in .proxy for  --dataType dns
# 2) DO end in .proxy for --dataType proxy
if params.get("dataType", "").lower() == "dns":
    subscriberFolders = [
        sub
        for sub in subscriberFolders
        if not sub.get("Prefix", "").endswith(PROXY_SUFFIX)
    ]
elif params.get("dataType", "").lower() == "proxy":
    subscriberFolders = [
        sub for sub in subscriberFolders if sub.get("Prefix", "").endswith(PROXY_SUFFIX)
    ]
print(subscriberFolders)

yearList = []
monthList = []
dayList = []
hourList = []

for subscriber in subscriberFolders:
    result = s3Client.list_objects_v2(
        Bucket=params["s3Bucket"],
        Delimiter="/",
        RequestPayer="requester",
        Prefix=subscriber.get("Prefix"),
    )
    yearList.extend(result.get("CommonPrefixes"))

for year in yearList:
    result = s3Client.list_objects_v2(
        Bucket=params["s3Bucket"],
        Delimiter="/",
        RequestPayer="requester",
        Prefix=year.get("Prefix"),
    )
    monthList.extend(result.get("CommonPrefixes"))
for month in monthList:
    result = s3Client.list_objects_v2(
        Bucket=params["s3Bucket"],
        Delimiter="/",
        RequestPayer="requester",
        Prefix=month.get("Prefix"),
    )
    dayList.extend(result.get("CommonPrefixes"))
for day in dayList:
    result = s3Client.list_objects_v2(
        Bucket=params["s3Bucket"],
        Delimiter="/",
        RequestPayer="requester",
        Prefix=day.get("Prefix"),
    )
    hourList.extend(result.get("CommonPrefixes"))

s3List = []
for thingType in hourList:
    string = thingType.get("Prefix")
    s3List.append(string.rstrip("/"))
# print("S3 Folder Structure At :")
# print(params['s3Bucket'] + '/' + params['s3Folder'])
# print("----------------------------------")
# print()
# print("S3 Partition List : ")
# print(s3List)
# print("----------------------------------")


# Compare Athena Partition List with S3 Partition List
resultSet = set(s3List) - set(athenaList)
# print("Result Set : ")
# print(resultSet)
# print("----------------------------------")
# print()


# Create Alter Query for Athena
if len(resultSet) != 0:
    queryString = (
        "ALTER TABLE "
        + params["tableName"]
        + " ADD IF NOT EXISTS PARTITION("
        + repr(resultSet)
    )
    queryString = queryString.replace("{", "")
    queryString = queryString.replace("}", "")
    queryString = queryString.replace("'", "")
    queryString = queryString.replace(",", "') PARTITION(")
    queryString = queryString.replace("subscriber=", "subscriber='")
    queryString = queryString.replace("year=", "year='")
    queryString = queryString.replace("month=", "month='")
    queryString = queryString.replace("day=", "day='")
    queryString = queryString.replace("hour=", "hour='")
    queryString = queryString.replace("/", "', ")
    queryString = queryString + "')"
    print("Alter Query String : ")
    print(queryString)
    print("----------------------------------")
    print()
    # Run Alter Partition Query
    execution = athena_query(athenaClient, queryString)
    if execution["ResponseMetadata"]["HTTPStatusCode"] == 200:
        # Temp Folder Cleanup
        cleanup(s3Resource, params)
        print("*~ SUCCESS ~*")
    else:
        print("#~ FAILURE ~#")
else:
    # Temp Folder Cleanup
    cleanup(s3Resource, params)
    print()
    print("*~ SUCCESS ~*")
