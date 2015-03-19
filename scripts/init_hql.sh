#!/bin/bash

HOME=/opt/bsb/sojourn
export TODAYSTR=`date '+%Y-%m-%d'`
USERID=`whoami`;
source "${HOME}/current/config/sojourn.common.properties";

if [ "${USERID}" != "hdfs" ]
then
   echo "Permission denied : Current User: ${USERID}       Permitted User : hdfs";
   echo "Script Aborted";
   exit;
fi