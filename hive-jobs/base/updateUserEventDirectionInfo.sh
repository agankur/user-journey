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
add jar ${HOME}/current/lib/UserJourney.jar ;

create temporary function sessionnum as 'com.bsb.portal.sojourn.hive.udf.SessionNum' ;
create temporary function sourceevent as 'com.bsb.portal.sojourn.hive.udf.SourceEvent' ;

INSERT OVERWRITE TABLE sj_user_event_direction_info  PARTITION(day='$DAYSTR')
SELECT user_id , sessionnum(timestr,user_id) as sessionNum ,sourceevent(timestr,user_id,event_id) as source_event_id,event_id as destination_event_id
FROM
(
    SELECT T1.user_id as user_id, T1.timestr as timestr,T2.id as event_id
    (
        SELECT DISTINCT user_id,screen_id,action_id,timestr 
        FROM twang.ext_analytics_log 
        WHERE (day = '$DAYSTR') AND (user_id IS NOT NULL) AND (timestr IS NOT NULL)
    )T1
    INNER JOIN 
    (
        SELECT * FROM sj_event_info
    )T2
    ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)
    ORDER BY user_id,timestr
)T3
WHERE source_event_id > 0;

"



#!/bin/bash
