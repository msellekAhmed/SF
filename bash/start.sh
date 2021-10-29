#!/bin/sh
  export SPARK_PRINT_LAUNCH_COMMAND=true
  export HDP_VERSION=2.8
  export SPARK_MAJOR_VERSION=2

 mainClass="com.socgen.poc.Anonymise"
 executorNum=${executorNum:-6}
 executorMemory=${executorMemory:-3g}
 executorCores=${executorCores:-4}
 driverMemory=${driverMemory:-4g}
 driverCores=${driverCores:-4}
 queue=${MAVEN_YARN_QUEUE}

 kerberosPrincipal=${MAVEN_KERBEROS_PRINCIPAL}
 kerberosKeytab=${MAVEN_KERBEROS_KEYTAB}
 jarName="MOTEUR-CONSERVATION-1.0-with-dependencies.jar"
 jksFileName=${MAVEN_JKS_FILE_NAME}
 lightbend="lightbend.conf"

# Initializing Kerberos Ticket
kinit -t ${kerberosKeytab} -k ${kerberosPrincipal}

spark-submit --keytab ${kerberosKeytab} \
             --principal ${kerberosPrincipal}  \
             --class ${mainClass} \
             --master yarn \
             --deploy-mode cluster \
             --queue ${queue} \
             --num-executors ${executorNum} \
             --executor-memory ${executorMemory} \
             --executor-cores ${executorCores} \
             --driver-memory ${driverMemory} \
             --driver-cores ${driverCores} \
             --files ${jksFileName} \
             --jars bc-fips-1.1.0.jar,bcmail-fips-1.0.0.jar,bcpg-fips-1.0.0.jar,bcpkix-fips-1.0.0.jar \
             --conf "spark.driver.extraClassPath=bc-fips-1.1.0.jar:bcmail-fips-1.0.0.jar:bcpg-fips-1.0.0.jar:bcpkix-fips-1.0.0.jar" \
             --conf "spark.executor.extraClassPath=bc-fips-1.1.0.jar:bcmail-fips-1.0.0.jar:bcpg-fips-1.0.0.jar:bcpkix-fips-1.0.0.jar" \
             --conf "spark.driver.extraJavaOptions=-Dconfig.file=lightbend.conf" \
             --conf "spark.executor.extraJavaOptions=-Dconfig.file=lightbend.conf" \
             ${jarName} \
             20190302 \
             201903 \
             run