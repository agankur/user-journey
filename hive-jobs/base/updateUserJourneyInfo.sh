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
INSERT OVERWRITE TABLE sj_user_journey_info  PARTITION(day='$DAYSTR')
SELECT T3.user_id as user_id, T3.event_id as event_id , collect_set(T3.timestr) as timestr_array,collect_set(T3.item_id) as song_id_array
FROM
(
    SELECT T1.user_id as user_id,T2.id as event_id,T1.item_id as item_id,T1.timestr as timestr
    FROM 
    (
        SELECT DISTINCT user_id,item_id,screen_id,action_id,timestr 
        FROM twang.ext_analytics_log 
        WHERE (day = '$DAYSTR' ) AND (user_id IS NOT NULL) AND (user_id != '-') AND (timestr IS NOT NULL)
    ) T1
    INNER JOIN 
    (
        SELECT * FROM sj_event_info
    ) T2
    ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)
    ORDER BY user_id,event_id,timestr
)T3
GROUP BY T3.user_id, T3.event_id

"



