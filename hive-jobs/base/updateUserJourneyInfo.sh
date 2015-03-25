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
hive -v -e "
USE sojourn;
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
          SELECT DISTINCT user_id,item_id,screen_id,action_id,server_timestr as timestr
          FROM twang.ext_analytics_log
          WHERE (day = '${DAYSTR}' ) AND (user_id IS NOT NULL) AND (user_id != '-') AND (server_timestr IS NOT NULL)
        "
done

echo "Processing User Jouney Info"
hive -v -e "
use sojourn;
INSERT OVERWRITE TABLE sj_user_journey_info
SELECT user_id,event_id,offset,time_diff_array,named_struct('item_id',item_array) as meta_info_array
FROM
(
    SELECT T5.user_id as user_id, T5.event_id as event_id ,T5.offset as offset, collect_list(T5.time_diff) as time_diff_array,collect_list(T5.item_id) as item_array
    FROM
    (
        SELECT T3.user_id as user_id ,T3.event_id as event_id,cast(T4.offset as BIGINT) as offset, cast((T3.timestr - T4.offset) as INT) as time_diff, T3.item_id as item_id
        FROM
        (
            SELECT T1.user_id as user_id,T2.id as event_id,T1.item_id as item_id,cast(T1.timestr/1000 as DOUBLE) as timestr
            FROM temp_user_journey T1 INNER JOIN sj_event_info T2
            ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)
        )T3, (SELECT MIN(cast(timestr/1000 as DOUBLE)) as offset FROM temp_user_journey)T4
        ORDER BY user_id,event_id,offset,time_diff
    )T5
    GROUP BY T5.user_id, T5.event_id,T5.offset 
)T6
;


DROP TABLE temp_user_journey;

"




