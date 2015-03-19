import org.apache.spark.sql.SchemaRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.catalyst.plans.{Inner,LeftOuter,JoinType}
import org.apache.spark.sql.catalyst.expressions.{IsNull, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.SparkContext._
import CommonFunctions._
import java.util.logging._

object DailyBaseJobs{
        def main(args: Array[String]) {
          val day = args(1)
          val tablesToAlter = if (args.length == 1) ALLJOBKEYLIST else args.slice(1,args.length)
          val sparkConf = new SparkConf().setAppName("dailyBaseJobs")
          val sc = new SparkContext(sparkConf)
          val hiveContext = new HiveContext(sc)
          import hiveContext._
          hiveContext.hql("use sojourn")
          val extAnalyticsLogTable = hiveContext.hql(s"SELECT T1.user_id as user_id,T2.id as event_id,T1.item_id as item_id,T1.timestr as timestr" +
            s"FROM (SELECT DISTINCT user_id,item_id,screen_id,action_id,timestr FROM twang.ext_analytics_log WHERE day = '$day' AND user_id IS NOT NULL) T1" +
            s"INNER JOIN (SELECT * FROM sj_event_info) T2" +
            s"ON (T1.action_id = T2.action_id) AND (T1.screen_id = T2.screen_id)")
          extAnalyticsLogTable.cache().collect()
          val activityTable = "dailyAnalyticsLog"
          extAnalyticsLogTable.registerTempTable(activityTable)
          for (job <- tablesToAlter) {
            runJob(getJobKey(job))
          }
          def runJob(jobKey: String): Unit = jobKey match {
            case "DailyUserJourney" => updateDailyUserJourney
            case "DailyUserEventDirection" => updateDailyUserEventDirection  
            case _ => return
          }
          
          def updateDailyUserJourney ={
             val tableToUpdate = "sj_user_journey_info" ;
             val userEventGroup = hiveContext.hql(s"SELECT user_id , event_id , collect_set(timestr),collect_set(item_id) FROM $activityTable GROUP BY user_id, event_id ")
             val userEventInfo = userEventGroup.map {case Row(user_id: String, event_id:String,timeArray: Array[Long],itemArray : Array[String]) => getUserEventInfo(user_id,event_id,timeArray,itemArray)} ;
             userEventInfo.collect()
             userEventInfo.registerTempTable("DailyUserJourney")
            (hiveContext.hql(s"INSERT OVERWRITE TABLE $tableToUpdate  PARTITION(day='$day') " +
              s"SELECT * FROM DailyUserJourney "))
          }
          
          def updateDailyUserEventDirection = {
            val tableToUpdate = "sj_user_event_direction_info" ;
            hiveContext.hql("create temporary function sessionnum as 'com.bsb.portal.sojourn.hive.udf.SessionNum' ")
            hiveContext.hql("create temporary function sourceevent as 'com.bsb.portal.sojourn.hive.udf.SourceEvent' ")
            val todayUsers = hiveContext.hql(s"SELECT user_id ,sessionnum(timestr,user_id) as sessionNum,sourceevent(timestr,user_id,event_id) as source_event_id,event_id as destination_event_id " +
              s"FROM (SELECT DISTINCT user_id,timestr,event_id FROM $activityTable ORDER BY user_id,timestr)T1 ")
            todayUsers.collect()
            todayUsers.registerTempTable("DailyUserEventDirection")
            (hiveContext.hql(s"INSERT OVERWRITE TABLE $tableToUpdate  PARTITION(day='$day') " +
              s"SELECT * FROM DailyUserEventDirection "))
            
          }
          

        }
}

