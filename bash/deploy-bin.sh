#!/bin/sh
##############################################################################
# FUNCTIONS
##############################################################################

# Function to help the user
usage() {
        echo -e "\r\tThis script deploy the Rebond Marketing CdN Project"
        echo -e "\tOPTIONS:"
        echo -e "\t--help       Help"
        echo -e "\t--env        Environnement: Dev, Int or Prod <dev/int/prd>   Ex: --env dev"
        exit 0;
}

# Function to define variables in function of parameters
defineVariable() {

        # Commun
        logDate=$(date +"%Y%m%d_%H%M%S")
        reportFile="pilote_conservation_${logDate}.log"
        error=0
        lastCmd=""
        appPath="/project/cdn/dec/dmp/conservation"

        if [[ ${env} == 'dev' ]]; then
                logPath="/applis/hadd/gcdl/gdpr/gdpr_test_meh/pilote/logs"
                oozieUrl="http://dhadlx102.haas.socgen:11000/oozie"
                kerberosPrincipal="pccpdadm@DDHAD"
                kerberosKeytab="/etc/security/keytabs/pccpdadm.DDHAD.applicatif.keytab"
        elif [[ ${env} == 'hom' ]]; then
                logPath="/applis/pccph/logs"
                oozieUrl="http://phadp30.haas.socgen:11000/oozie"
                kerberosPrincipal="pccphadm/phadlx56.haas.socgen@PHAD"
                kerberosKeytab="/applis/pccph/.keytabs/pccphadm.applicatif.keytab"
        elif [[ ${env} == 'prd' ]]; then
                logPath=~
                oozieUrl="http://phadlx52.haas.socgen:11000/oozie"
                kerberosPrincipal="pccppadm/phadlx52.haas.socgen@PHAD"
                kerberosKeytab="/applis/pccpp/.keytabs/pccppadm.applicatif.keytab"
        else
               usage
        fi
}

# Function to initialize kerberos ticket
initializeKerberosTicket() {
        # Int and Prd
        if [[ ${env} == 'int' ]] || [[ ${env} == 'prd' ]] || [[ ${env} == 'dev' ]]; then
                log "Logging to Kerberos as ${kerberosPrincipal} using ${kerberosKeytab}"
                execute "kinit -k ${kerberosPrincipal} -t ${kerberosKeytab}"
                check
        fi
}

# Function to log messages
log() {
        echo -e "$(date +"%d/%m/%Y %H:%M:%S") [INFO] $1" | tee -a ${logPath}/${reportFile}
        lastCmd="$1"
}

# Function to execute  actions and log them
execute () {
        echo -e "$(date +"%d/%m/%Y %H:%M:%S") [EXEC] $1" | tee -a ${logPath}/${reportFile}
        $1 1>> ${logPath}/${reportFile} 2>&1
}

# Function to check last action success
check() {
        if (( $? != 0 )); then
                echo -e "$(date +"%d/%m/%Y %H:%M:%S") [ERROR] $lastCmd [KO]" | tee -a ${logPath}/${reportFile}
                let error=error+1
        fi
}

# Function to get parameters on launch
getArguments() {
        OPTS=$(getopt -o o --long env:, -- "$@")
        if [ $? != 0 ]; then usage; fi;

        eval set -- "$OPTS"

        while true ; do
            case "$1" in
                --help) usage; echo u; shift 2;;
                --env)  env=${2}; shift 2;;
                --) shift; break;;
            esac
        done
}

# Function to welcome the user
hello() {
        log  "You are running the script $0 for the enseigne cdn."
}

# Function to trap exit signal
end(){
        log "${error} errors have been detected during the execution."
        log "For more information, please consult: ${logPath}/${reportFile}"
}


##############################################################################
# INITIALIZATION
##############################################################################

getArguments "$@"
defineVariable
initializeKerberosTicket

trap end EXIT

##############################################################################
# DEPLOY
##############################################################################
# Welcome message
hello

# Upload Coordinator Resources
log "Uploading Coordinator Resources"
execute "hdfs dfs -mkdir -p ${appPath}"
execute "hdfs dfs -put -f $(dirname $0)/apps ${appPath}"
execute "hdfs dfs -put -f $(dirname $0)/lib ${appPath}"
check

# Trigger Oozie Bundle
log "Triggering Oozie Bundle"
execute "oozie job -config $(dirname $0)/apps/oozie/coordinator/coordinator.properties -run -oozie ${oozieUrl}"
check