"""
Glue Spark job to create a timeframe-bounded CSV of policy triggers from an Athena table.
Each row identifies a single (epoch, policy, IP, DNS requested hostname) tuple.
If a single request triggered multiple policies, then multiple rows will appended to the CSV
(i.e., if a single request from IP 10.2.3.4 at 1612304100 for the domain malware.bad
    triggers the pair of policies sb-phishing-page-1 and sb-infected-page-1, then
    two rows would be created:
        1612304100000000,2021-02-02,20:15,subscriber,sb-phishing-page-1,10.2.3.4,malware.bad
        1612304100000000,2021-02-02,20:15,subscriber,sb-infected-page-1,10.2.3.4,malware.bad
Output fields:
    epoch_microsec,date_utc,time_utc,policy,client_ip,dns_question
Required params:
    --region            | AWS region where Athena table or view resides. Should be us-gov-west-1
    --athenaDatabase    | Athena database containing table or view from which to read
    --athenaTable       | Athena table or view from which to read
    --outputBucket      | S3 bucket where CSV results should be stored
    One (but not both) of --dayRange or --maxHoursAgo is also required.
Optional params:
    --dayRange          | Static range of days from which data should be read. Should be
                          formatted as YYYYMMDD-YYYYMMDD (e.g., 20201230-20210119).
                          Cannot be used with --maxHoursAgo
    --maxHoursAgo       | CSV's maximum data age in hours (i.e., if set to 12, then the
                          CSV will contain data that is up to 12 hourly partitions older than
                          the current time.
                          Cannot be used with --dayRange.
    --fullDays          | When set in conjunction with --maxHoursAgo, all days in CSV
                          prior to the current day will be complete.
                          Setting to any value == True; not setting at all == False
    --policies          | Policies to target for feed. Should be a delimiter-joined string
                          such as a CSV string.
                          Default when not set: sb-phishing-page-2,sb-infected-page-2
    --delimiter         | Delimiter used to separate --policies parameter.
                          Default when not set: ,
    --outputDir         | S3 directory path (not including bucket name) where CSV results should be written.
                          Default when not set: PolicyTriggerCSV-<current_epoch>-<random_string>
    --verbose           | Prints more verbose output to Glue logs
Will fail if:
 * athenaDatabase.athenaTable does not exist or cannot be accessed,
 * outputBucket does not exist or cannot be accessed,
 * BOTH dayRange and maxHoursAgo were set,
 * NEITHER dayRange nor maxHoursAgo was set, or
 * a job parameter is improperly formatted.
"""
import sys
import boto3
from os import path
from types import SimpleNamespace
from time import time, sleep
from random import choice
from datetime import datetime, timedelta
from string import ascii_lowercase, digits
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import explode, col, from_unixtime

REQUIRED_PARAMS = [
    "region",
    "athenaDatabase",
    "athenaTable",
    "outputBucket",
    "JOB_NAME",
]
OPTIONAL_PARAMS = [
    "dayRange",
    "maxHoursAgo",
    "fullDays",
    "policies",
    "delimiter",
    "outputDir",
    "verbose",
]
PARAM_START_INDEX = 1
START_TIME = int(time())
HOURLY_ALIGNER = 60 * 60
DAILY_ALIGNER = HOURLY_ALIGNER * 24
# Boto keys
B = {
    "QEID": "QueryExecutionId",
    "QE": "QueryExecution",
    "STATUS": "Status",
    "STATE": "State",
    "QUEUED": "QUEUED",
    "RUNNING": "RUNNING",
    "SUCCEEDED": "SUCCEEDED",
    "SCR": "StateChangeReason",
    "RESP_META": "ResponseMetadata",
    "HTTP_STATUS": "HTTPStatusCode",
    "ERROR": "Error",
    "CODE": "Code",
    "OUTPUT": "OutputLocation",
}
B = SimpleNamespace(**B)
DEFAULTS = {
    "policies": "sb-phishing-page-2,sb-infected-page-2",
    "delimiter": ",",
    "outputDir": "PolicyTriggerCSV-%s-%s"
    % (START_TIME, "".join([choice(ascii_lowercase + digits) for ch in range(8)])),
}


class ValidationException(Exception):
    pass


def validate_db(database, region, create_db=False):
    glue_client = boto3.client("glue", region_name=region)
    # Check to see if Athena database exists.
    db_resp = 0
    try:
        db_resp = glue_client.get_database(Name=database)[B.RESP_META][B.HTTP_STATUS]
    except ClientError as ce:
        db_resp = ce.response[B.RESP_META][B.HTTP_STATUS]
    # Only acceptable outcome: 200 (database exists and we can access it)
    if not db_resp == 200:
        raise ValidationException(
            "Could not verify database %s exists: %s" % (database, db_resp)
        )
    return True


def validate_table(database, table, region):
    glue_client = boto3.client("glue", region_name=region)
    # Check to see if Athena table/view exists.
    table_resp = 0
    try:
        table_resp = glue_client.get_table(DatabaseName=database, Name=table)[
            B.RESP_META
        ][B.HTTP_STATUS]
    except ClientError as ce:
        table_resp = ce.response[B.RESP_META][B.HTTP_STATUS]
    # Only acceptable outcome: 200 (table exists and we can access it)
    if not table_resp == 200:
        # Try to be helpful on the error type if we can.
        if table_resp == 400:
            raise ValidationException(
                "Table/view %s.%s could not be found: %s"
                % (database, table, table_resp)
            )
        elif table_resp == 403:
            raise ValidationException(
                "Table/view %s.%s is not authorized: %s" % (database, table, table_resp)
            )
        else:
            raise ValidationException(
                "Received unacceptable HTTP response code for table/view "
                "%s.%s: %s. Desired response: 200" % (database, table, table_resp)
            )
    # No problems encountered if we made it this far
    return True


def validate_bucket(bucket, region):
    s3_client = boto3.client("s3", region_name=region)
    # Check if bucket exists.
    # Only acceptable outcome: 200 (bucket exists and we can access it)
    http_resp = 0
    try:
        http_resp = (
            s3_client.head_bucket(Bucket=bucket)
            .get(B.RESP_META, {})
            .get(B.HTTP_STATUS, 0)
        )
    # Catch non-200 responses.
    except ClientError as ce:
        http_resp = int(ce.response[B.ERROR][B.CODE])
    if not http_resp == 200:
        raise ValidationException(
            "Could not verify bucket s3://%s exists and is accessible: %s"
            % (bucket, http_resp)
        )
    return True


def get_args():
    # Required parameters can be easily retrieved.
    args = getResolvedOptions(sys.argv, REQUIRED_PARAMS)
    # Optional parameters require slightly more effort.
    raw_params = sys.argv[PARAM_START_INDEX:]
    param_pairs = dict(
        [raw_params[index : index + 2] for index in range(0, len(raw_params), 2)]
    )
    for opt in OPTIONAL_PARAMS:
        args[opt] = param_pairs.get("--%s" % (opt), DEFAULTS.get(opt, None))
    # Validate exactly one of maxHoursAgo and dayRange is set.
    if args.get("maxHoursAgo") and args.get("dayRange"):
        raise ValidationException("--maxHoursAgo and --dayRange cannot both be set.")
    elif not args.get("maxHoursAgo") and not args.get("dayRange"):
        raise ValidationException("Either --maxHoursAgo or --dayRange must be set.")
    # Validate maxHoursAgo param (if present).
    if args.get("maxHoursAgo") is not None:
        try:
            args["maxHoursAgo"] = int(args["maxHoursAgo"])
        except:
            raise ValidationException(
                "If set, --maxHoursAgo must be an integer. Value received %s"
                % (args["maxHoursAgo"])
            )
        if args["maxHoursAgo"] < 0:
            raise ValidationException("--max_hours_ago cannot be under 0.")
        elif not args["maxHoursAgo"]:
            print(
                "WARNING: maxHoursAgo set to 0. View will read only data for the current hourly partition."
            )
    # Validate dayRange param (if present).
    elif args.get("dayRange"):
        try:
            start, end = args.get("dayRange").split("-")
            start_dt = datetime.strptime(start, "%Y%m%d")
            end_dt = datetime.strptime(end, "%Y%m%d")
            args["startDt"] = start_dt
            args["endDt"] = end_dt
        except:
            raise ValidationException(
                "Invalid --day_range received: %s" % (args.day_range)
            )
        if args["startDt"] > args["endDt"]:
            raise ValidationException(
                "Invalid --dayRange received: start date cannot be later than end date."
            )
    args = SimpleNamespace(**args)
    # Validate we received only bucket names.
    if args.outputBucket.lower().startswith("s3://"):
        raise ValidationException(
            "Bucket params must be bucket names only (i.e., not start with s3://)."
        )
    elif args.outputBucket.endswith("/"):
        raise ValidationException(
            "Bucket params must not end with trailing slash (i.e., / )."
        )
    return args


def main(args):
    if args.verbose:
        print("Got arguments: %s" % (args))
    # Verify source DB and table exist
    if validate_db(args.athenaDatabase, args.region) and args.verbose:
        print("Validated source database %s exists." % (args.athenaDatabase))
    if (
        validate_table(args.athenaDatabase, args.athenaTable, args.region)
        and args.verbose
    ):
        print("Validated source table %s exists." % (args.athenaTable))
    # Verify output bucket exists and is accessible.
    if validate_bucket(args.outputBucket, args.region) and args.verbose:
        print("Verified bucket s3://%s exists and is accessible." % (args.outputBucket))
    # Get timeframe for pushdown predicate
    pushdown = ""
    if args.maxHoursAgo:
        aligner = DAILY_ALIGNER if args.fullDays else HOURLY_ALIGNER
        min_hour = ((START_TIME - (3600 * args.maxHoursAgo)) // aligner) * aligner
        pushdown = "(hour >= %s)" % (min_hour)
    elif args.dayRange:
        min_epoch = int((args.startDt - datetime.utcfromtimestamp(0)).total_seconds())
        # Hour boundary is first hourly epoch after last specified day.
        max_epoch = int(
            (
                (args.endDt + timedelta(days=1)) - datetime.utcfromtimestamp(0)
            ).total_seconds()
        )
        query += "(hour >= %s and hour < %s)" % (min_epoch, max_epoch)
    # Get targeted policies.
    # Should be passed to SparkSQL as quoted strings
    policies = ", ".join(
        ["'%s'" % (pol) for pol in args.policies.split(args.delimiter) if pol]
    )
    sc = SparkContext()
    gc = GlueContext(sc)
    job = Job(gc)
    job.init(args.JOB_NAME, vars(args))
    raw_data = gc.create_dynamic_frame.from_catalog(
        database=args.athenaDatabase,
        table_name=args.athenaTable,
        transformation_ctx="raw_data",
        push_down_predicate=pushdown,
    )
    df = (
        raw_data.toDF()
        .filter("policies is not NULL")
        .select(
            "start_time",
            from_unixtime(col("start_time") / 1000000, "yyyy-MM-dd").alias("datestamp"),
            from_unixtime(col("start_time") / 1000000, "HH:mm:ss").alias("timestamp"),
            "subscriber",
            "client_address",
            "dns_question_name",
            explode("policies").alias("policy"),
        )
        .filter("policy in (%s)" % (policies))
        .orderBy("start_time")
        .coalesce(1)
    )
    write_frame = DynamicFrame.fromDF(df, gc, "transformed_frame")
    s3_loc = "s3://%s/%s" % (args.outputBucket, args.outputDir)
    data_sink = gc.write_dynamic_frame.from_options(
        frame=write_frame,
        connection_type="s3",
        connection_options={"path": s3_loc},
        format="csv",
        transformation_ctx="data_sink",
    )
    job.commit()


if __name__ == "__main__":
    args = get_args()
    main(args)
