#!/bin/bash
#set -x

##############################################################################
#
# Script to run Spark Scala job.
#
##############################################################################
HOME="/opt/bsb/sojourn";
source "${HOME}/current/scripts/common_func.sh";
SPARK_HOME="/opt/spark";
JOB_CLASS="DailyBaseJobs";
JOB_JAR="${HOME}/current/lib/scala-2.10/user-journey-basejobs_2.10-1.0.jar"
JOB_DEP_JARS="${HOME}/current/lib/json-serde-1.3.1.jar,${HOME}/current/lib/UserJourney.jar";
DAYSTR=$1;
NUM_EXECUTOR=$2;
ARGS=( $@ );
NUM_ARGS=${#ARGS[@]};
JOBS_TO_RUN=${ARGS[@]:2:$NUM_ARGS};
##############################################################################


##############################################################################
#
# Function MAIN()
#
##############################################################################
function MAIN()
{
   loggerInfo "***      Script: $0   START  ***";
   if [ -z ${DAYSTR} ]
   then
      DAYSTR=`date -d "1 day ago" '+%Y-%m-%d'`
   fi
   monthStr=`echo ${DAYSTR} | awk -F"-" '{print $1"-"$2}'`
   if [ -z ${NUM_EXECUTOR} ]
   then
      NUM_EXECUTOR=5
   fi
   loggerInfo "Running Job -  ${JOB_CLASS} : ${SPARK_HOME}/bin/spark-submit --master yarn-client --class ${JOB_CLASS} --jars ${JOB_DEP_JARS} --num-executors ${NUM_EXECUTOR} --executor-memory 4G  ${JOB_JAR} ${monthStr} ${DAYSTR} ${JOBS_TO_RUN}"
   ${SPARK_HOME}/bin/spark-submit --master yarn-client --class ${JOB_CLASS} --jars ${JOB_DEP_JARS} --num-executors ${NUM_EXECUTOR} --executor-memory 4G ${JOB_JAR} ${monthStr} ${DAYSTR} ${JOBS_TO_RUN} ;
   loggerInfo "***      Script: $0    END   ***";

}
##############################################################################


##############################################################################
#
# Script Entry
#
##############################################################################
MAIN $@;

