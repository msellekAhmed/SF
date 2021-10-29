
################################DEF PARAMS###############################
#!/bin/bash

initParams(){
INPUT_FILE="/applis/hadd/gcdl/gdpr/gdpr_test_meh/conservation/test.xlsx"
OUTPUT_FILE="/applis/hadd/gcdl/gdpr/gdpr_test_meh/conservation/out.csv"
SOURCE_PATH="/applis/hadd/gcdl/gdpr/gdpr_test_meh/conservation/com.socgen.converter-1.0-SNAPSHOT/*jar"
HDFS_LOCATION="/project/cdn/gdpr/hive/declensh/"
JVM_PATH=""
USER="gdprpdadm@DDHAD"
KEYTAB="/etc/security/keytabs/gdprpdadm.DDHAD.applicatif.keytab"
}

runDeclensh(){
java  -cp $SOURCE_PATH com.socgen.XlsxToCSV.Converter $INPUT_FILE  $OUTPUT_FILE
kinit  $USER -kt  $KEYTAB
hdfs dfs -put -f  $OUTPUT_FILE $HDFS_LOCATION
DATE=`date +%Y%m`
PARTITION="$DATE"
hdfs dfs -touchz /project/cdn/gdpr/hive/declensh/PARTITION
rm $OUTPUT_FILE
}

