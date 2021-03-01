#! /usr/bin/env python3

"""
Recommended Glue environment:
    Type: Python shell
    Python version: Python 3 (Glue Version 1.0)

Glue Python job to create a timeframe-bounded CSV of policy or parent policy triggers from an Athena table.
Each row identifies a single policy hit captured by Shepherd DNS.
If a single request triggered multiple policies or parent policies, then multiple rows will appended to the CSV
(i.e., if a single request from IP 10.2.3.4 at 1612304100 for the domain malware.bad
    triggers the pair of parent policies sb-phishing-page and sb-infected-page, then
    two rows would be created:
        1612304100000000,pm-resolver,10.2.3.4, ... malware.bad, ... sb-phishing-page, ...
        1612304100000000,pm-resolver,10.2.3.4, ... malware.bad, ... sb-infected-page, ...

Output fields:
    Output fields will correspond to all available columns in the current version of the Shepherd data
    dictionary (including partition columns) with the exception of the policies / parent_policies column
    (depending on use of the --policies or --parentPolicies param, respectively). The targeted
    [parent_]policies column will instead be expanded to produce one string field per array element
    as discussed in the section above.

Required params:
    --region            | AWS region where Athena table or view resides. Should be us-gov-west-1
    --athenaDatabase    | Athena database containing table or view from which to read
    --athenaTable       | Athena table or view from which to read
    --outputBucket      | S3 bucket where CSV results should be stored
    --salt              | A random set of characters used as a salt in hashing algorithms
    --ordinal           | An ordinal related to the subscriber
    --subscriber        | The name of the subscriber
    --receiver          | The email address of the receiver
    --workgroup         | Name of Athena Workgroup against which queries should be run. The selected workgroup
                          should support requester-payer settings.
                          Default: primary
    One (but not both) of --dayRange or --maxHoursAgo is required.
    One (but not both) of --parentPolicies or --policies is required.
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
    --parentPolicies    | Policies to target for feed. Should be a delimiter-joined string
                          such as a CSV string.
                          Cannot be set if --policies is also set.
    --policies          | Policies (non-parent) to target for feed. Should be a delimiter-joined
                          string such as a CSV string.
                          Cannot be set if --parentPolicies is also set.
    --delimiter         | Delimiter used to separate --policies/--parentPolicies parameter.
                          Default when not set: ,
    --outputDir         | S3 directory path (not including bucket name) where CSV results should be written.
                          Default when not set: PolicyTriggerCSV-<current_epoch>-<random_string>
    --outputFilename    | When set, rename output file to given string.
                          Directory location will be preserved.
    --deleteMetadataFile | When set, the .csv.metadata file produced alongside the .csv query output
                           will be automatically deleted. Deleting this file does not affect the expected
                           .csv output file, nor does it affect the underlying Shepherd data in S3.
                           When not set, the .csv.metadata file will remain in the S3 location specified
                           by the --outputBucket and --outputDir params.
    --deleteOrigOnRename   | When set to any value in conjunction with --outputFilename, creates a renamed COPY
                             of output file and DELETES original. if --outputFilename is set and
                             --deleteOrigOnRename is not, then BOTH the renamed version of the file
                             and the original copy of saved.
    --timeout_sec       | Number of seconds after which Athena query attempt should timeout and fail.
                          Default: 1800 (30 minutes)
    --verbose           | Prints more verbose output to Glue logs.
Will fail if:
 * athenaDatabase.athenaTable does not exist or cannot be accessed,
 * outputBucket does not exist or cannot be accessed,
 * workgroup is invalid or has improper permissions,
 * timeout_sec is 0 or less,
 * BOTH policies and parentPolicies were set,
 * NEITHER policies not parentPolicies was set,
 * BOTH dayRange and maxHoursAgo were set,
 * NEITHER dayRange nor maxHoursAgo was set, or
 * a job parameter is improperly formatted.
"""

import hashlib
import os.path
import sys
from datetime import datetime, timedelta
from random import choice
from string import ascii_lowercase, digits
from time import time, sleep
from types import SimpleNamespace

import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


REQUIRED_PARAMS = [
    "region",
    "athenaDatabase",
    "athenaTable",
    "outputBucket",
    "salt",
    "ordinal",
    "subscriber",
    "receiver",
    "workgroup",
]
OPTIONAL_PARAMS = [
    "dayRange",
    "maxHoursAgo",
    "fullDays",
    "policies",
    "parentPolicies",
    "delimiter",
    "outputDir",
    "verbose",
    "outputFilename",
    "timeout_sec",
    "deleteMetadataFile",
    "deleteOrigOnRename",
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
    "STATS": "Statistics",
    "QUEUED": "QUEUED",
    "RUNNING": "RUNNING",
    "SUCCEEDED": "SUCCEEDED",
    "SCR": "StateChangeReason",
    "RESP_META": "ResponseMetadata",
    "HTTP_STATUS": "HTTPStatusCode",
    "ERROR": "Error",
    "CODE": "Code",
    "OUTPUT": "OutputLocation",
    "RC": "ResultConfiguration",
    "TIME_IN_MS": "EngineExecutionTimeInMillis",
}
B = SimpleNamespace(**B)

DEFAULTS = {
    "delimiter": ",",
    "timeout_sec": 1800,
    "workgroup": "primary",
    "outputDir": "PolicyTriggerCSV-%s-%s"
    % (START_TIME, "".join([choice(ascii_lowercase + digits) for ch in range(8)])),
}


class ValidationException(Exception):
    pass


class TimeoutException(Exception):
    pass


class OutputException(Exception):
    pass


class QueryException(Exception):
    def __init__(self, message, reason="UnknownReason"):
        self.message = message
        self.reason = reason

    def __str__(self):
        return str(self.message)


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


def delete_s3_obj(full_path):
    if full_path.startswith("s3://"):
        full_path = full_path[len("s3://") :]
    bucket, key = full_path.split(os.path.sep, 1)

    s3_client = boto3.resource("s3")
    delete_resp = s3_client.Object(bucket, key).delete()
    if not delete_resp.get(B.RESP_META, {}).get(B.HTTP_STATUS, None) == 204:
        raise Exception(
            "Received non-204 response on delete attempt: %s" % (delete_resp)
        )

    return True


def execute_query(query, query_output_loc, args):

    if args.verbose:
        print("Running query: %s" % (query))

    start_time = int(time())
    athena_client = boto3.client("athena", region_name=args.region)
    result = {}

    run_me = athena_client.start_query_execution(
        QueryString=query,
        WorkGroup=args.workgroup,
        ResultConfiguration={
            B.OUTPUT: query_output_loc
        },  # Workgroup will override this setting
    )

    if args.verbose:
        print("Query is QUEUED")
    while (
        athena_client.get_query_execution(QueryExecutionId=run_me[B.QEID])[B.QE][
            B.STATUS
        ][B.STATE]
        == B.QUEUED
    ):
        sleep(1)
        if (int(time()) - start_time) >= args.timeout_sec:
            raise TimeoutException(
                "Query timed out during queue stage after %s seconds."
                % (args.timeout_sec)
            )

    if args.verbose:
        print("Query is RUNNING")
    while (
        athena_client.get_query_execution(QueryExecutionId=run_me[B.QEID])[B.QE][
            B.STATUS
        ][B.STATE]
        == B.RUNNING
    ):
        sleep(1)
        if (int(time()) - start_time) >= args.timeout_sec:
            raise TimeoutException(
                "Query timed out during execution stage after %s seconds."
                % (args.timeout_sec)
            )

    if (
        not athena_client.get_query_execution(QueryExecutionId=run_me[B.QEID])[B.QE][
            B.STATUS
        ][B.STATE]
        == B.SUCCEEDED
    ):
        if args.verbose:
            print("Query is FAILED")
        result = athena_client.get_query_execution(QueryExecutionId=run_me[B.QEID])[
            B.QE
        ][B.STATUS].get(B.SCR, "UnknownQueryFailure")
        raise QueryException("Query %s failed: %s" % (query, result), reason=result)

    # Query succeeded
    else:
        if args.verbose:
            print("Query is SUCCEEDED")
        return athena_client.get_query_execution(QueryExecutionId=run_me[B.QEID])


def get_file_contents(s3_path, region):
    s3_client = boto3.client("s3", region_name=region)
    if s3_path.startswith("s3://"):
        s3_path = s3_path[len("s3://") :]
    bucket, key = s3_path.split(os.path.sep, 1)

    info = s3_client.get_object(Bucket=bucket, Key=key)
    if not info.get("Body", None):
        raise OutputException(
            "S3 location %s does not exist or contains no content." % (s3_path)
        )

    return info.get("Body").read().decode("utf-8")


def get_args():
    # Required parameters can be easily retrieved.
    args = getResolvedOptions(sys.argv, REQUIRED_PARAMS)

    # Optional parameters require slightly more effort.
    param_pairs = {}
    for p in sys.argv[PARAM_START_INDEX:]:
        key, value = p.split("=", 1)
        param_pairs[key.strip("-")] = value

    for opt in OPTIONAL_PARAMS:
        args[opt] = param_pairs.get(opt, DEFAULTS.get(opt, None))

    # Validate exactly one of maxHoursAgo and dayRange is set.
    if args.get("maxHoursAgo") and args.get("dayRange"):
        raise ValidationException("--maxHoursAgo and --dayRange cannot both be set.")
    elif not args.get("maxHoursAgo") and not args.get("dayRange"):
        raise ValidationException("Either --maxHoursAgo or --dayRange must be set.")

    # Validate exactly one of policies and parentPolicies is set.
    if args.get("policies") and args.get("parentPolicies"):
        raise ValidationException("--policies and --parentPolicies cannot both be set.")
    elif not args.get("policies") and not args.get("parentPolicies"):
        raise ValidationException("Either --policies or --parentPolicies must be set.")
    args["policy_type"] = "policies" if args.get("policies") else "parent_policies"
    args["policy_strings"] = (
        args.get("policies") if args.get("policies") else args.get("parentPolicies")
    )

    # Validate maxHoursAgo param (if present).
    if args.get("maxHoursAgo") is not None:
        try:
            args["maxHoursAgo"] = int(args["maxHoursAgo"])
        except Exception:
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
        except Exception:
            raise ValidationException(
                "Invalid --day_range received: %s" % (args.day_range)
            )
        if args["startDt"] > args["endDt"]:
            raise ValidationException(
                "Invalid --dayRange received: start date cannot be later than end date."
            )

    # Validate timeout_sec is an integer greater than 0
    try:
        args["timeout_sec"] = int(args["timeout_sec"])
    except Exception:
        raise ValidationException(
            "Invalid --timeout_sec received: value must be an integer."
        )
    if not args["timeout_sec"] > 0:
        raise ValidationException(
            "Invalid --timeout_sec received: value must be greater than 0."
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


def hash_key(salt, ordinal, subscriber, receiver):
    salt_bytes = bytes(salt, "utf-8")
    ordinal_bytes = bytes(ordinal, "utf-8")
    subscriber_bytes = bytes(subscriber, "utf-8")
    receiver_bytes = bytes(receiver, "utf-8")
    dk = hashlib.pbkdf2_hmac(
        "sha512", subscriber_bytes + receiver_bytes, salt_bytes + ordinal_bytes, 100000
    )
    return dk.hex()


def main(args):
    if args.verbose:
        print("Got arguments: %s" % (args))
        print()

    # Verify source DB and table exist
    if validate_db(args.athenaDatabase, args.region) and args.verbose:
        print("Validated source database %s exists." % (args.athenaDatabase))
        print()
    if (
        validate_table(args.athenaDatabase, args.athenaTable, args.region)
        and args.verbose
    ):
        print("Validated source table %s exists." % (args.athenaTable))
        print()

    # Verify output bucket exists and is accessible.
    if validate_bucket(args.outputBucket, args.region) and args.verbose:
        print("Verified bucket s3://%s exists and is accessible." % (args.outputBucket))
        print()

    uniq = hash_key(args.salt, args.ordinal, args.subscriber, args.receiver)
    uniq_dir = os.path.join(args.outputDir, uniq)
    s3_loc = "s3://%s" % (os.path.join(args.outputBucket, uniq_dir))

    # Get table fields from `describe` DDL query.
    field_query_res = execute_query(
        "describe %s.%s" % (args.athenaDatabase, args.athenaTable), s3_loc, args
    )
    field_query_loc = field_query_res.get(B.QE, {}).get(B.RC, {}).get(B.OUTPUT, None)
    if not field_query_loc:
        raise QueryException(
            "Unable to retrieve fields for table %s.%s."
            % (args.athenaDatabase, args.athenaTable)
        )

    # Retrieve results of DDL query.
    spec = get_file_contents(field_query_loc, args.region)
    fields = []
    for elem in spec.split("\n"):
        field = elem.strip()
        if not field:
            break
        fields.append(field.split().pop(0))
    # Delete outputs of DDL query.
    # delete_txt
    delete_s3_obj(field_query_loc)
    # delete_metadata
    delete_s3_obj("%s.metadata" % (field_query_loc))

    # Get timeframe
    pushdown = ""
    if args.maxHoursAgo:
        aligner = DAILY_ALIGNER if args.fullDays else HOURLY_ALIGNER
        min_hour = ((START_TIME - (3600 * args.maxHoursAgo)) // aligner) * aligner
        pushdown = "hour >= %s" % (min_hour)

    elif args.dayRange:
        min_epoch = int((args.startDt - datetime.utcfromtimestamp(0)).total_seconds())
        # Hour boundary is first hourly epoch after last specified day.
        max_epoch = int(
            (
                (args.endDt + timedelta(days=1)) - datetime.utcfromtimestamp(0)
            ).total_seconds()
        )
        pushdown = "hour >= %s and hour < %s" % (min_epoch, max_epoch)

    # Get targeted policies or parentPolicies.
    # Should be passed to Athena as quoted strings.
    pol_arr = ", ".join(
        ["'%s'" % (pol) for pol in args.policy_strings.split(args.delimiter) if pol]
    )

    # Construct query. policies / parent_policies column will be exploded and aliased as "policy",
    # while all other fields will be preserved.
    fields = [f if f != args.policy_type else "policy" for f in fields]
    query = (
        'select * from (select %s from "%s"."%s" cross join unnest(%s) as t (policy)'
        % (", ".join(fields), args.athenaDatabase, args.athenaTable, args.policy_type)
    )
    query += " where %s is not null and %s)a" % (args.policy_type, pushdown)
    query += " where a.policy in (%s)" % (pol_arr)

    # Execute CSV-generating query.
    csv_query_res = execute_query(query, s3_loc, args)
    csv_query_loc = csv_query_res.get(B.QE, {}).get(B.RC, {}).get(B.OUTPUT, None)
    if not csv_query_loc:
        raise QueryException(
            "Unable to retrieve results for CSV generating query against %s.%s."
            % (args.athenaDatabase, args.athenaTable)
        )
    if args.verbose:
        print(
            "Wrote CSV query results to %s. Query completed in %s seconds."
            % (
                csv_query_loc,
                csv_query_res.get(B.QE, {})
                .get(B.STATS, {})
                .get(B.TIME_IN_MS, "unknown"),
            )
        )

    # Rename output file, if requested.
    if args.outputFilename:

        # This is the final object that will get renamed.
        # Athena appears to only produce a single output CSV irrespective of size,
        # so checking for multiple objects is unnecessary.
        # output_obj = None

        # Copy the object, which retains the original
        obj_info = csv_query_loc
        if obj_info.startswith("s3://"):
            obj_info = obj_info[len("s3://") :]
        bucket, old_key = obj_info.split(os.path.sep, 1)
        bucket_dir = os.path.dirname(old_key)
        new_key = os.path.join(bucket_dir, args.outputFilename)
        s3_resource = boto3.resource("s3", region_name=args.region)
        copy_resp = s3_resource.Object(bucket, new_key).copy_from(
            CopySource=os.path.join(bucket, old_key)
        )

        if not copy_resp.get(B.RESP_META, {}).get(B.HTTP_STATUS, None) == 200:
            raise Exception(
                "Received non-200 response on copy attempt: %s" % (copy_resp)
            )

        if args.verbose:
            print(
                "Renamed file from s3://%s to s3://%s"
                % (
                    os.path.join(bucket, old_key),
                    os.path.join(bucket, new_key),
                )
            )

        new_key = os.path.join(uniq_dir, args.outputFilename)
        copy_resp = s3_resource.Object(args.outputBucket, new_key).copy_from(
            CopySource=os.path.join(bucket, old_key)
        )

        if not copy_resp.get(B.RESP_META, {}).get(B.HTTP_STATUS, None) == 200:
            raise Exception(
                "Received non-200 response on copy attempt: %s" % (copy_resp)
            )

        if args.verbose:
            print(
                "Renamed file from s3://%s to s3://%s"
                % (
                    os.path.join(bucket, old_key),
                    os.path.join(args.outputBucket, new_key),
                )
            )

        # Delete original copy of file (if requested).
        if args.deleteOrigOnRename:
            # delete_orig
            delete_s3_obj(csv_query_loc)
            if args.verbose:
                print("Deleted original copy of CSV at %s." % (csv_query_loc))

    # Delete metadata file (if requested).
    if args.deleteMetadataFile:
        # delete_metadata
        delete_s3_obj("%s.metadata" % (csv_query_loc))


if __name__ == "__main__":
    args = get_args()
    main(args)
