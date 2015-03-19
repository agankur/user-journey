#!/bin/sh
#set -x
##############################################################################
#
# Script to send notification email
#
##############################################################################
HOME="/opt/bsb/sojourn";
source "${HOME}/current/scripts/common_func.sh";
EMAIL_TO="pramods@bsb.in,ankura@bsb.in";
EMAIL_FROM="SOJOURN Hive Production";
EMAIL_SUB="Hive Job Alert : ";
EMAIL_BODY="/tmp/tempEmailBody";
##############################################################################


##############################################################################
#
# Function MAIN()
#
##############################################################################
function MAIN()
{
   loggerInfo "***     Script:$0     START    ***";
   NOTIFICATION_TYPE=$1;
   shift;
   NOTIFICATION_DATA=$1;
   shift;
   NOTIFICATION_ADD_INFO=$*;
   
   if [ "${NOTIFICATION_TYPE}" == "INFO" ]
   then
      EMAIL_SUB="INFO : ${NOTIFICATION_DATA}";
   elif [ "${NOTIFICATION_TYPE}" == "BASE" ]
   then 
      EMAIL_SUB="FATAL ERROR : ${NOTIFICATION_DATA}";
   else
      EMAIL_SUB="JOB FAILURE : ${NOTIFICATION_DATA}";
   fi
   
   TIMESTR=`date`;
   if [ "${NOTIFICATION_TYPE}" == "BASE" ]
   then
      echo "*****  SOJOURN Analytics System  *****" > ${EMAIL_BODY}
      echo "Notification Type : BASE Data Load Failed - Fatal Error" >> ${EMAIL_BODY}
      echo "Date/Time : ${TIMESTR}" >> ${EMAIL_BODY}
      echo "Additional Info : ${NOTIFICATION_ADD_INFO}" >> ${EMAIL_BODY}
   elif [ "${NOTIFICATION_TYPE}" == "INFO"  ]
   then
      echo "*****  SOJOURN Analytics System  *****" > ${EMAIL_BODY}
      echo "Notification Type : DATA Sync - Information" >> ${EMAIL_BODY}
      echo "Date/Time : ${TIMESTR}" >> ${EMAIL_BODY}
      echo "Additional Info : ${NOTIFICATION_ADD_INFO}" >> ${EMAIL_BODY}
   else
     echo "*****  SOJOURN Analytics System  *****" >  ${EMAIL_BODY}
     echo "Notification Type : REPORT Failed Error" >> ${EMAIL_BODY}
     echo "Date/Time : ${TIMESTR}" >> ${EMAIL_BODY}
     echo "Additional Info : ${NOTIFICATION_ADD_INFO}" >> ${EMAIL_BODY}
   fi
   loggerInfo "Sending Notification : ${EMAIL_TO}";
   emailto "${EMAIL_TO}" "${EMAIL_FROM}" "${EMAIL_SUB}" "${EMAIL_BODY}"
   loggerInfo "Removing temp data : ${EMAIL_BODY}";
   rm -rf ${EMAIL_BODY};
   loggerInfo "***     Script:$0     END    ***";
}
##############################################################################

##############################################################################
#
# Script Entry
#
##############################################################################
MAIN $@;

