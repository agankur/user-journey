#!/bin/bash

HOME="/opt/bsb/sojourn";
source "${HOME}/current/scripts/common_func.sh";
source "${HOME}/current/scripts/init_hql.sh";
DAYSTR=$1

if [  -z  ${DAYSTR} ]
then
      DAYSTR=`date -d "1 day ago" '+%Y-%m-%d'`
fi

echo "Running for DAYSTR=${DAYSTR}";
hive DAYSTR=${DAYSTR} -d HOME=${HOME} -v -e "
use sojourn;
LOAD DATA LOCAL INPATH '${HOME}/current/lib/eventIdData' OVERWRITE INTO TABLE sj_event_info;
"



