"""
Glue Spark job to *update* an existing CSV of DNS policy triggers from an Athena table.
Reads existing CSV, removes rows that are out of time range, queries for new
rows, and appends newly collected data to updated copy of CSV.
This approach reduces the time and costs that would be incurred by a job
that repeatedly queried a multi-hour or multi-day timeframe on an hourly basis.

Each row identifies a single policy hit captured by Shepherd DNS.
If a single request triggered multiple policies, then multiple rows will appended to the CSV
(i.e., if a single request from IP 10.2.3.4 at 1628426940000 for the domain malware.bad
    triggers the pair of policies sb-phishing-page-1 and sb-infected-page-1, then
    two rows would be created:
        1628426940000000,pm-resolver,10.2.3.4, ... malware.bad, ... sb-phishing-page, ...
        1612304100000000,pm-resolver,10.2.3.4, ... malware.bad, ... sb-infected-page, ...

Output fields:
    Output fields will correspond to all available columns in the current version of the Shepherd DNS
    data dictionary (including partition columns) with the exception of the policies / parent_policies
    column (depending on use of the --policies or --parentPolicies param, respectively). The targeted
    [parent_]policies column will instead be expanded to produce one string field per array element
    as discussed in the section above.
    Proxy policy triggers are not included in this version of the Glue job.

Required params:
    --region            | AWS region where source Athena table or view resides. Should be us-gov-west-1
    --athenaDatabase    | Athena database containing source table or view
    --athenaTable       | Athena table or view from which to read
    --inputBucket       | S3 bucket where previous CSV results are stored.
    --salt              | A random set of characters used as a salt in hashing algorithms
    --ordinal           | An ordinal related to the subscriber
    --subscriber        | The name of the subscriber
    --receiver          | The email address of the receiver

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
    --hourly            | When set, assumes job is run once per hour instead of once
                          per day. Checks only for new triggers in current and previous
                          hour (versus standard daily behavior, which checks for new
                          policy triggers in current and previous 24 hours).
    --parentPolicies    | Policies to target for feed. Should be a delimiter-joined string
                          such as a CSV string.
                          Cannot be set if --policies is also set.
    --policies          | Policies (non-parent) to target for feed. Should be a delimiter-joined
                          string such as a CSV string.
                          Cannot be set if --parentPolicies is also set.
    --delimiter         | Delimiter used to separate --policies parameter.
                          Default when not set: ,
    --outputBucket      | S3 bucket where CSV results should be stored.
                          When not set, uses the same value as --inputBucket parameter.
    --inputPrefix       | Optional S3 prefix to filter CSV file candidates in --inputBucket.
                          May be a directory, file substring (top-level directory only), or a combination
                          of the two. The most recently written file matching the prefix string will be
                          selected as the input CSV on which the updated file is based.
                          When not set, all files in input CSV bucket will be treated as possible candidates
                          for input CSV, and the most recently written file in the bucket will be selected
                          as the input file.
    --outputDir         | S3 directory path (not including bucket name) where CSV results should be written.
                          Default when not set: PolicyTriggerCSV-<current_epoch>-<random_string>
    --outputFilename    | When set, rename output file to given string.
                          Directory location will be preserved.
    --dontPreserveOutputDir | When set with --outputFilename, directory location will not be preserved
                              (i.e., setting no directory pathing will place output file in top level of S3
                              bucket, setting --dontPreserveOutputDir to some_dir/LatestTriggers.csv will
                              place it in some_dir, etc).
    --keepOrigOnRename  | When set to any value in conjunction with --outputFilename, creates a renamed COPY
                          of output file and preserves original. if --outputFilename is set and
                          --keepOrigOnRename is not, then only the renamed version of the file is saved.
    --nonStrict         | When set, starting point for incremental query will be the greater of
                          --maxHoursAgo and of the last seen hourly epoch in the input CSV.
                          When not set, starting point in incremental query will be
                          the previous hourly epoch in real time (based on job start time).
                          Example 1: For a job submitted on 9 Feb 2021 at 12:20 UTC against an input CSV
                            ith a most recent policy trigger of 9 Feb 2021 9:32 UTC and a --maxHoursAgo of 12:
                            --nonStrict not set (default): earliest epoch to query is 1612868400 (11:00 UTC)
                            --nonStrict IS set: earliest epoch to query is 1612861200 (9:00 UTC).
                          Example 2: For a job submitted on 9 Feb 2021 at 12:20 UTC against an input CSV
                            ith a most recent policy trigger of 9 Feb 2021 5:14 UTC and a --maxHoursAgo of 5:
                            --nonStrict not set (default): earliest epoch to query is 1612868400 (11:00 UTC)
                            --nonStrict IS set: earliest epoch to query is 1612854000 (7:00 UTC).
                            (The previous hit fell outside of the --maxHoursAgo range, which permitted a
                             minimum epoch of 1612854000 / 7:00 UTC.)
                          Setting --nonStrict may result in more expensive queries, but allows the CSV update
                          job to fill in missing data if previous runs of the script failed or if the Glue job
                          was suspended.
                          Not setting --nonStrict is generally recommended if the Glue job is expected to run
                          every hour.
    --verbose           | Prints more verbose output to Glue logs

Will fail if:
 * athenaDatabase.athenaTable does not exist or cannot be accessed,
 * outputBucket does not exist or cannot be accessed,
 * inputBucket is set, but does not exist or cannot be accessed,
 * no suitable input files are found in the inputBucket,
 * if inputDir is set, not suitable input files are found matching the inputDir prefix,
 * BOTH policies and parentPolicies were set,
 * NEITHER policies not parentPolicies was set,
 * BOTH dayRange and maxHoursAgo were set,
 * NEITHER dayRange nor maxHoursAgo was set, or
 * maxHoursAgo is an invalid value, or
 * a job parameter is improperly formatted.
"""

import hashlib
import os.path
import sys
import boto3
from types import SimpleNamespace
from time import time
from random import choice
from string import ascii_lowercase, digits
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row

REQUIRED_PARAMS = [
    "region",
    "athenaDatabase",
    "athenaTable",
    "inputBucket",
    "salt",
    "ordinal",
    "subscriber",
    "receiver",
    "JOB_NAME",
]
OPTIONAL_PARAMS = [
    "dayRange",
    "maxHoursAgo",
    "fullDays",
    "hourly",
    "parentPolicies",
    "policies",
    "delimiter",
    "outputBucket",
    "inputPrefix",
    "outputDir",
    "outputFilename",
    "dontPreserveOutputDir",
    "nonStrict",
    "keepOrigOnRename",
    "verbose",
]
PARAM_START_INDEX = 1
FN_INDEX = 0
DT_INDEX = 1
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
    "TABLE": "Table",
    "STORAGE": "StorageDescriptor",
    "COL": "Columns",
    "P_KEYS": "PartitionKeys",
    "NAME": "Name",
    "TYPE": "Type",
}
B = SimpleNamespace(**B)

DEFAULTS = {
    "delimiter": ",",
    "outputDir": "PolicyTriggerCSV-%s-%s"
    % (START_TIME, "".join([choice(ascii_lowercase + digits) for ch in range(8)])),
}


class ValidationException(Exception):
    pass


def validate_db(database, create_db=False, glue_client=None, region_name=None):
    if not glue_client:
        glue_client = boto3.client("glue", region_name=region_name)

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


def validate_table(database, table, glue_client=None, region_name=None):
    if not glue_client:
        glue_client = boto3.client("glue", region_name=region_name)

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


def validate_bucket(bucket, s3_client=None, region_name=None):
    if not s3_client:
        s3_client = boto3.client("s3", region_name=region_name)

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


# Returns tuple (file name, file write datetime),
# or (None, None) if no candidate file found.
def get_latest_file(bucket, prefix=None, s3_client=None, region_name=None):
    if not s3_client:
        s3_client = boto3.client("s3", region_name=region_name)

    prefix = "" if not prefix else prefix
    target_file = None
    paginator = s3_client.get_paginator("list_objects_v2")
    for chunk in paginator.paginate(Bucket=bucket, Prefix=prefix):
        files = [
            (fn.get("Key", ""), fn.get("LastModified", 0))
            for fn in chunk.get("Contents", [])
            if fn.get("Key", "").endswith(".csv")
        ]
        latest_file = max(files, key=lambda fn: fn[DT_INDEX])
        if not target_file or latest_file[DT_INDEX] > target_file[DT_INDEX]:
            target_file = latest_file
    return target_file if target_file else (None, None)


# bucket arg refers to both input and output bucket.
def rename_file(
    bucket,
    rename_fn,
    prefix=None,
    dont_preserve_dir=True,
    keep_orig=False,
    s3_client=None,
    region_name=None,
):
    if not s3_client:
        s3_client = boto3.client("s3", region_name=region_name)

    prefix = "" if not prefix else prefix
    s3_resource = boto3.resource("s3", region_name=region_name)

    # Rename latest file matching prefix
    candidate_fn = get_latest_file(bucket, prefix=prefix, s3_client=s3_client)[FN_INDEX]
    if not candidate_fn:
        raise Exception(
            "Unable to find job output file in s3://%s/%s" % (bucket, prefix)
        )

    rename_dir = "%s/" % (prefix)
    if args.dontPreserveOutputDir:
        rename_dir = ""

    copy_resp = s3_resource.Object(bucket, "%s%s" % (rename_dir, rename_fn)).copy_from(
        CopySource="%s/%s" % (bucket, candidate_fn)
    )
    if not copy_resp.get(B.RESP_META, {}).get(B.HTTP_STATUS, None) == 200:
        raise Exception("Received non-200 response on copy attempt: %s" % (copy_resp))

    if not keep_orig:
        delete_resp = s3_resource.Object(bucket, candidate_fn).delete()
        if not delete_resp.get(B.RESP_META, {}).get(B.HTTP_STATUS, None) == 204:
            raise Exception(
                "Received non-204 response on delete attempt: %s" % (delete_resp)
            )

    # No errors if we've made it this far
    return True


def get_table_schema(database, table, glue_client=None, region_name=None):
    if not glue_client:
        glue_client = boto3.client("glue", region_name=region_name)

    resp = glue_client.get_table(DatabaseName=database, Name=table)
    if not resp.get(B.RESP_META, {}).get(B.HTTP_STATUS, None) == 200:
        raise Exception("Received non-200 response on Glue request: %s" % (resp))

    cols = resp.get(B.TABLE, {}).get(B.STORAGE, {}).get(B.COL, [])
    p_cols = resp.get(B.TABLE, {}).get(B.P_KEYS, [])
    return [(col.get(B.NAME), col.get(B.TYPE)) for col in cols] + [
        (p_col.get(B.NAME), p_col.get(B.TYPE)) for p_col in p_cols
    ]


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

    # If outputBucket not set, use inputBucket
    if not args.get("outputBucket"):
        args["outputBucket"] = args["inputBucket"]

    # Validate exactly one of policies and parentPolicies is set.
    if args.get("policies") and args.get("parentPolicies"):
        raise ValidationException("--policies and --parentPolicies cannot both be set.")
    elif not args.get("policies") and not args.get("parentPolicies"):
        raise ValidationException("Either --policies or --parentPolicies must be set.")
    args["policyType"] = "policies" if args.get("policies") else "parent_policies"
    args["policyStrings"] = (
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
            raise ValidationException("--maxHoursAgo cannot be under 0.")
        elif not args["maxHoursAgo"]:
            print(
                "WARNING: maxHoursAgo set to 0. View will read only data for the current hourly partition."
            )

    args = SimpleNamespace(**args)

    # Validate we received only bucket names.
    if args.outputBucket.lower().startswith(
        "s3://"
    ) or args.inputBucket.lower().startswith("s3://"):
        raise ValidationException(
            "Bucket params must be bucket names only (i.e., not start with s3://)."
        )
    elif args.outputBucket.endswith("/") or args.inputBucket.endswith("/"):
        raise ValidationException(
            "Bucket params must not end with trailing slash (i.e., / )."
        )

    return args


def json_clean(inp):
    try:
        if not inp:
            return inp
        if isinstance(inp, dict) or isinstance(inp, Row):
            d = ""
            for k in inp:
                d += "%s=%s, " % (k, inp[k])
            return "{%s}" % (d[:-2])
        elif isinstance(inp, list):
            build_res = []
            for elem in inp:
                if isinstance(elem, Row):
                    elem = elem.asDict()
                d = ""
                for k in elem:
                    d += "%s=%s, " % (k, elem[k])
                d = "{%s}" % (d[:-2])
                build_res.append(d)
            return "[%s]" % (", ".join(build_res))
        else:
            return "error"
    except Exception as e:
        return "%s - %s" % (e, inp)


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

    glue_client = boto3.client("glue", region_name=args.region)
    s3_client = boto3.client("s3", region_name=args.region)

    # Verify source DB and table exist
    if validate_db(args.athenaDatabase, glue_client=glue_client) and args.verbose:
        print("Validated source database %s exists." % (args.athenaDatabase))
    if (
        validate_table(args.athenaDatabase, args.athenaTable, glue_client=glue_client)
        and args.verbose
    ):
        print("Validated source table %s exists." % (args.athenaTable))

    # Verify input and output buckets exist and are accessible.
    for bucket in [args.outputBucket, args.inputBucket]:
        if validate_bucket(bucket, s3_client=s3_client) and args.verbose:
            print(
                "Verified bucket s3://%s exists and is accessible."
                % (args.outputBucket)
            )

    # Use latest file in bucket that matches prefix string as input.
    input_csv, latest_dt = get_latest_file(
        args.inputBucket, prefix=args.inputPrefix, s3_client=s3_client
    )

    if not input_csv:
        raise Exception(
            "Found no candidate CSV files in bucket %s with prefix %s."
            % (
                args.inputBucket,
                "(no prefix)" if not args.inputPrefix else args.inputPrefix,
            )
        )

    print(
        "Got latest CSV file s3://%s/%s with write time %s."
        % (args.inputBucket, input_csv, latest_dt)
    )

    sc = SparkContext()
    gc = GlueContext(sc)
    sparkSession = gc.spark_session
    job = Job(gc)
    job.init(args.JOB_NAME, vars(args))
    sparkSession.udf.register("json_clean", json_clean)

    # For requester payer
    sparkSession._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader", "true")
    gc._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader", "true")

    # Get minimum hour for existing data
    prelim_min_hour = 0
    aligner = DAILY_ALIGNER if args.fullDays else HOURLY_ALIGNER
    # * 1000000 needed since start_time is in microseconds
    prelim_min_hour = (
        ((START_TIME - (3600 * args.maxHoursAgo)) // aligner) * aligner
    ) * 1000000

    # Get CSV file contents, filtering out lines that don't fall within --maxHoursAgo.
    input_df = (
        gc.create_dynamic_frame_from_options(
            "s3",
            {"paths": ["s3://%s/%s" % (args.inputBucket, input_csv)]},
            "csv",
            {"withHeader": True},
        )
        .toDF()
        .select("*")
    )
    print("Input DF count: %s" % (input_df.count()))
    csv_prelim_df = input_df.filter(
        "cast(start_time as bigint) >= %s" % (prelim_min_hour)
    )
    print("Date-bounded DF count: %s" % (csv_prelim_df.count()))

    # If --hourly is not set, query for policy hits for current hour
    # and previous 24 hours.
    # When --hourly is set, query for policy hits only for the current
    # and previous hour. (Querying for the previous hour is necessary)
    # as the previous CSV generation job may have run in the middle of the
    # hour, meaning new events for that hour may exist.)
    # (now - (now % 3600)) = start of the current hour
    # ((now - (now % 3600)) - 3600) = start of the previous hour
    hours_ago = 1 if args.hourly else 24
    increm_min_hour_sec = (START_TIME - (START_TIME % 3600)) - (3600 * hours_ago)
    increm_min_hour_micro = increm_min_hour_sec * 1000000

    # When nonStrict is set, query for the latest hourly epoch in the input file
    # as well as all subsequent hourly epochs. This query may be more expensive
    # to complete, but allows for the possibility of filling in gaps if previous
    # runs of the job failed or were paused.
    if args.nonStrict:
        # We may need to re-query for transactions in the latest epoch depending on
        # when the previous query was run. Filter these rows out as well, then
        # set newest_epoch to our minimum hour epoch.
        last_seen_epoch = (
            input_df.selectExpr("cast(start_time as bigint) as ts")
            .agg({"start_time": "max"})
            .collect()
            .pop()[0]
        )
        last_seen_hour = ((last_seen_epoch // 1000000) // aligner) * aligner
        # Make sure we don't go outside our --maxHoursAgo range.
        # This check is necessary as increm_min_hour_sec is used in the pushdown
        # predicate when querying against the Athena source table/view.
        increm_min_hour_sec = max(last_seen_hour, increm_min_hour_sec)
        increm_min_hour_micro = increm_min_hour_sec * 1000000

    print("Set minimum hour in seconds: %s" % (increm_min_hour_sec))

    csv_hits_in_range = csv_prelim_df.filter(
        "cast(start_time as bigint) < %s" % (increm_min_hour_micro)
    )
    print("Total CSV hits in timeframe of interest: %s" % (csv_hits_in_range.count()))

    # Set timeframe for pushdown predicate
    pushdown = "(hour >= %s)" % (increm_min_hour_sec)

    # Get table contents starting from minimum hour settings.
    raw_data = gc.create_dynamic_frame.from_catalog(
        database=args.athenaDatabase,
        table_name=args.athenaTable,
        transformation_ctx="raw_data",
        push_down_predicate=pushdown,
    )

    # Determine what fields we should extract based on table definition.
    # Deriving the fields from the table schema is preferable to inferring
    # it from the underlying parquet because the two may differ.
    tmp_schema = get_table_schema(
        args.athenaDatabase, args.athenaTable, glue_client=glue_client
    )
    # Verify we only have one matching column on which to explode.
    if not [col[0] for col in tmp_schema].count(args.policyType) == 1:
        raise Exception(
            "Wrong number of matching %s columns found in schema: %s"
            % (args.policyType, tmp_schema)
        )
    # Sub out the policies/parent_policies field for an explode expression.
    # Lightly modify other fields to promote clean and consistent writes to CSV file.
    schema = []
    for col in tmp_schema:
        if col[0] == args.policyType:
            schema.append("explode(%s) as policy" % (col[0]))
        elif "map" in col[1].lower() or "struct" in col[1].lower():
            schema.append("json_clean(%s) as %s" % (col[0], col[0]))
        else:
            schema.append("cast(%s as string)" % (col[0]))

    # Get targeted policies/parent_policies.
    # Should be passed to SparkSQL as quoted strings.
    pol_arr = ", ".join(
        ["'%s'" % (pol) for pol in args.policyStrings.split(args.delimiter) if pol]
    )

    # Filter table contents according to policy hits.
    # A non-null policies field implies a non-null parent_policies field
    # (and vice versa), so it's OK to filter on just one.

    new_hits = (
        raw_data.toDF()
        .filter("policies is not NULL")
        .selectExpr(*schema)
        .filter("policy in (%s)" % (pol_arr))
    )
    print("New policy triggers found since last job: %s" % (new_hits.count()))

    # Combine newly collected policy hits with dataframe of previous CSV contents.
    write_df = new_hits.union(csv_hits_in_range).orderBy("start_time").coalesce(1)

    uniq = hash_key(args.salt, args.ordinal, args.subscriber, args.receiver)
    s3_loc = "s3://%s" % (os.path.join(args.outputBucket, args.outputDir, uniq))
    if args.verbose:
        print("S3 Results Location: %s" % s3_loc)

    write_df.write.option("quoteAll", True).csv(s3_loc, header=True)

    # Rename output file, if requested.
    if args.outputFilename:
        rename_resp = rename_file(
            args.outputBucket,
            args.outputFilename,
            prefix=args.outputDir,
            dont_preserve_dir=args.dontPreserveOutputDir,
            keep_orig=args.keepOrigOnRename,
        )

        if rename_resp and args.verbose:
            print("Renamed file to %s/%s." % (args.outputDir, args.outputFilename))

    job.commit()


if __name__ == "__main__":
    args = get_args()
    main(args)
    print("Finished job.")
