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

echo "Creating Temporary table for storing weeks data from activity logs"
hive -v - e "
USE use sojourn;
DROP TABLE IF EXISTS temp_user_journey;
CREATE TABLE temp_user_journey (
     user_id                 STRING,
     item_id                 STRING,
     screen_id               STRING,
     action_id               STRING,
     timestr                 BIGINT)
STORED as orc;
"

now=`date +"%Y-%m-%d" -d "$DAYSTR - 7 days"`;
echo "Updating Temporary table with a weeks data  starting with : ${now} and ending with : ${DAYSTR}"

while [ "${now}" != "${DAYSTR}" ] ;
do
        now=`date +"%Y-%m-%d" -d "$now + 1 day"`;
        hive DAYSTR=${now} -v -e "
          use sojourn;
          INSERT INTO TABLE  temp_user_journey
          SELECT DISTINCT user_id,item_id,screen_id,action_id,timestr 
          FROM twang.ext_analytics_log
          WHERE (day = '$DAYSTR' ) AND (user_id IS NOT NULL) AND (user_id != '-') AND (timestr IS NOT NULL)  
        "
done

echo "Processing User Jouney Info"
hive -v -e "
use sojourn;
INSERT OVERWRITE TABLE sj_user_journey_info
SELECT T4.user_id as user_id, T4.event_id as event_id , collect_list(T4.time_diff) as time_diff_array,collect_list(T4.meta_info) as meta_info_array
FROM
(
    SELECT user_id,event_id,offset, (timestr - offset) as time_diff, named_struct('item_id',item_id) as meta_info
    FROM
    (
        SELECT T1.user_id as user_id,T2.id as event_id,T1.item_id as item_id,T1.timestr as timestr, MIN(timestr) as offset
        FROM temp_user_journey T1 INNER JOIN sj_event_info T2
        ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)
        ORDER BY user_id,event_id,timestr
    )T3
)T4
GROUP BY T4.user_id, T4.event_id ;


DROP TABLE temp_user_journey;

"




