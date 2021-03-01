#! /usr/bin/env bash

#
# This is a script to test the CSV script by setting flags similar to the Glue ETL job
#

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "${SALT}" ]; then
  SALT=$(chamber read shepherd-global salt -q)
fi

# AWS Glue passes in all values as strings without equal signs. Repeat that here:
"${DIR}"/create_csv.py \
    "--region" "us-gov-west-1" \
    "--athenaDatabase" "shepherd_global_database_sub_dod_dds_r8cf2j5q" \
    "--athenaTable" "dns_data" \
    "--parentPolicies" "sb-infected-page,sb-phishing-page,sb-safe-search-youtube,sb-safe-search,sb-restricted-schedule,sb-whitelist" \
    "--maxHoursAgo" "720" \
    "--fullDays" "true" \
    "--outputBucket" "jwkoam2f.dds.mil" \
    "--outputDir" "csv" \
    "--outputFilename" "interesting.csv" \
    "--salt" "${SALT}" \
    "--ordinal" "0" \
    "--subscriber" "sub.dod.dds" \
    "--receiver" "eli@dds.mil,chris.gilmer@dds.mil" \
    "--verbose" "true" \
    "--timeout_sec" "1800" \
    "--deleteMetadataFile" "true" \
    "--workgroup" "shepherd-global-workgroup-sub.dod.dds-r8cf2j5q"

