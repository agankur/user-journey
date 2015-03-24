#!/bin/bash

HOME="/opt/bsb/sojourn";
source "${HOME}/current/scripts/common_func.sh";
source "${HOME}/current/scripts/init_hql.sh";

loggerInfo "Creating Schema: sojourn";
hive -v -e "CREATE SCHEMA IF NOT EXISTS sojourn LOCATION '/apps/hive/warehouse/sojourn/'";
checkStatusANDErrMsgExit "ERROR : Creating Schema";

hive -v -d HOME=${HOME} -e "
use sojourn;
add jar $HOME/current/lib/json-serde-1.3.1.jar ;
add jar $HOME/current/lib/UserJourney.jar;

CREATE TABLE IF NOT EXISTS sj_event_info(
    id                      INT,
    screen_id               STRING,
    action_id               STRING,
    description             STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
    
CREATE TABLE IF NOT EXISTS sj_user_journey_info(
    user_id                 STRING,
    event_id                INT,
    offset                  BIGINT
    time_diff_array         ARRAY<INT>,
    meta_info_array         ARRAY<STRUCT<STRING:STRING>>)
   STORED as orc;

CREATE TABLE IF NOT EXISTS sj_user_event_direction_info(
   user_id                  STRING,
   begin_day                STRING,
   end_day                  STRING,
   unique_bi_event_array    ARRAY<STRUCT<STRING:INT>>)
  STORED as orc;

 "

