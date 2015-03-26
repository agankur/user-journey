#!/bin/bash

HOME="/opt/bsb/sojourn";
source "${HOME}/current/scripts/common_func.sh";
source "${HOME}/current/scripts/init_hql.sh";
DAYSTR=$1

if [  -z  ${DAYSTR} ]
then
      DAYSTR=`date -d "1 day ago" '+%Y-%m-%d'`
fi

echo "Creating Temporary table for storing weeks data from activity logs"
hive DAYSTR=${DAYSTR} -d HOME=${HOME} -v -e "
use sojourn;
DROP TABLE IF EXISTS temp_user_event_direction;
CREATE TABLE temp_user_event_direction (
     user_id                 STRING,
     screen_id               STRING,
     action_id               STRING,
     timestr                 BIGINT)
STORED as orc;
"

START_DAY=`date +"%Y-%m-%d" -d "$DAYSTR - 6 days"`;
now=`date +"%Y-%m-%d" -d "$DAYSTR - 7 days"`; 
echo "Updating Temporary table with a weeks data  starting with : ${START_DAY} and ending with : ${DAYSTR}"

while [ "${now}" != "${DAYSTR}" ] ;
do
        now=`date +"%Y-%m-%d" -d "$now + 1 day"`;
        hive -d DATE=${now} -v -e '
          use sojourn;
          INSERT INTO TABLE  temp_user_event_direction
          SELECT DISTINCT user_id,screen_id,action_id,server_timestr as timestr
          FROM twang.ext_analytics_log
          WHERE (day = "${DATE}" ) AND (user_id IS NOT NULL) AND (user_id != "-") AND (server_timestr IS NOT NULL)
        '
done

echo "Processing Event Direction Info Info"

hive -d SDAY=$START_DAY -d EDAY=$DAYSTR -d HOME=$HOME -v -e '
use sojourn;
add jar ${HOME}/current/lib/UserJourney.jar ;
create temporary function sourceevent as "com.bsb.portal.sojourn.hive.udf.SourceEvent" ;

INSERT OVERWRITE TABLE sj_user_event_direction_info PARTITION(end_day = "${EDAY}" , start_day = "${SDAY}")
SELECT user_id,source_event_id,destination_event_id,count(*)
FROM
(
    SELECT DISTINCT T3.user_id as user_id ,T3.timestr as timestr,sourceevent(T3.timestr,T3.user_id,T3.event_id) as source_event_id,T3.event_id as destination_event_id 
    FROM
    (
        SELECT T1.user_id as user_id,T2.id as event_id,T1.timestr as timestr
        FROM temp_user_event_direction T1 INNER JOIN sj_event_info T2
        ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)
        ORDER BY user_id,timestr
    )T3
)T4
WHERE source_event_id > 0
GROUP BY user_id,source_event_id,destination_event_id ;



DROP TABLE temp_user_event_direction;

'


