#!/bin/bash
DAYSTR=$1

if [  -z  ${DAYSTR} ]
then
      DAYSTR=`date -d "1 day ago" '+%Y-%m-%d'`
fi

echo "Running for DAYSTR=${DAYSTR}";

echo "Creating Temporary table for storing weeks data from activity logs"
hive -v -e '
USE sojourn;
DROP TABLE IF EXISTS temp_user_journey;
CREATE TABLE temp_user_journey (
     user_id                 STRING,
     item_id                 STRING,
     screen_id               STRING,
     action_id               STRING,
     timestr                 BIGINT)
STORED as orc;
'

START_DAY=`date +"%Y-%m-%d" -d "$DAYSTR - 6 days"`;
now=`date +"%Y-%m-%d" -d "$DAYSTR - 7 days"`; 
echo "Updating Temporary table with a weeks data  starting with : ${START_DAY} and ending with : ${DAYSTR}"

while [ "${now}" != "${DAYSTR}" ] 
do
        now=`date +"%Y-%m-%d" -d "$now + 1 day"`;
        hive -d DATE=${now} -v -e '
          use sojourn;
          INSERT INTO TABLE  temp_user_journey
          SELECT DISTINCT user_id,item_id,screen_id,action_id,server_timestr as timestr
          FROM twang.ext_analytics_log
          WHERE (day = "${DATE}" ) AND (user_id IS NOT NULL) AND (user_id != "-") AND (server_timestr IS NOT NULL)
        '
done

echo "Processing User Jouney Info"
hive -d SDAY=$START_DAY -d EDAY=$DAYSTR -v -e '
use sojourn;
INSERT OVERWRITE TABLE user_journey_info PARTITION(end_day = "${EDAY}" , start_day = "${SDAY}")
SELECT user_id,event_id,time_diff_array,map("item_id",item_array) as meta_info_array
FROM
(
    SELECT T5.user_id as user_id, T5.event_id as event_id ,collect_list(T5.time_diff) as time_diff_array,collect_list(T5.item_id) as item_array
    FROM
    (
        SELECT T3.user_id as user_id ,T3.event_id as event_id, cast((T3.timestr - 1262284200) as INT) as time_diff, T3.item_id as item_id
        FROM
        (
            SELECT T1.user_id as user_id,T2.id as event_id,T1.item_id as item_id,cast(T1.timestr/1000 as DOUBLE) as timestr
            FROM temp_user_journey T1 INNER JOIN sj_event_info T2
            ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)
        )T3
        ORDER BY user_id,event_id,time_diff
    )T5
    GROUP BY T5.user_id, T5.event_id 
)T6;


DROP TABLE temp_user_journey;

'




