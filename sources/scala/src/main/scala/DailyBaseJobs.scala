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
import java.io._


object DailyBaseJobs{
        def main(args: Array[String]) {
          val start_day = args(0)
          val end_day = args(1)
          val tablesToAlter = if (args.length == 2) ALLJOBKEYLIST else args.slice(2,args.length)
          val sparkConf = new SparkConf().setAppName("populateGraph")
          val sc = new SparkContext(sparkConf)
          val hiveContext = new HiveContext(sc)
          import hiveContext._
          hiveContext.hql("use sojourn")
          val summaryTable = hiveContext.hql(s"SELECT * FROM sj_user_event_direction_info WHERE start_day= '$start_day' AND end_day = '$end_day' ")
          summaryTable.cache()
          val eventTable = "eventDirectionInfo"
          summaryTable.registerTempTable(eventTable)

          for (job <- tablesToAlter) {
            runJob(job);
          }
          
          def runJob(jobKey: String): Unit = jobKey match {
            case "TotalSummary" => updateTotalSummary
            case "DownloadSummary" => updateDownloadSummary
            case _ => return
          }
          
          def updateTotalSummary = {
            val totalSummary = hiveContext.hql(s"SELECT source_event_id,destination_event_id,sum(count) as count FROM  $eventTable GROUP BY source_event_id,destination_event_id ")
            val graphInfo = totalSummary.map ( t => (t(0),t(1),t(2)) ).collect
            val totalSum  = graphInfo.map(_._3).reduceLeft(_ + _);
            val weightedGraph = graphInfo.map { case (x,y,z) => (x,y,1- (z*1.0/totalSum)) }
            val numEdges = weightedGraph.length
            val numVertices = weightedGraph.flatMap{ case(a,b,c) => List(a,b) }.toSet.size 
            val textFile = new File("/opt/bsb/sojourn/current/resources/totalSummary.txt");
            if (!textFile.exists()) {
              textFile.createNewFile();
            }
            val fw = new FileWriter(textFile.getAbsoluteFile());
            val bw = new BufferedWriter(fw);
            bw.write(numVertices  + "\n" + numEdges + "\n")
            weightedGraph.foreach {case (x,y,z) => bw.write(x + " " + y +" "+ z + "\n")}
            bw.close()

          }
          
          def updateDownloadSummary = {
            val totalSummary = hiveContext.hql(s"SELECT T1.source_event_id as source_event_id,T1.destination_event_id as destination_event_id,sum(count) as count " +
              s"FROM $eventTable T1 INNER JOIN " +
              s"(SELECT DISTINCT user_id FROM $eventTable where destination_event_id = 311 )T2 " +
              s"ON T1.user_id = T2.user_id" +
              s"GROUP BY T1.source_event_id,T1.destination_event_id")
            val graphInfo = totalSummary.map ( t => (t(0),t(1),t(2)) ).collect
            val totalSum  = graphInfo.map(_._3).reduceLeft(_ + _);
            val weightedGraph = graphInfo.map{ case (x,y,z) => (x,y,1- (z*1.0/totalSum)) }
            val numEdges = weightedGraph.length
            val numVertices = weightedGraph.flatMap{ case(a,b,c) => List(a,b)}.toSet.size
            val textFile = new File("/opt/bsb/sojourn/current/resources/downloadSummary.txt");
            if (!textFile.exists()) {
              textFile.createNewFile();
            }
            val fw = new FileWriter(textFile.getAbsoluteFile());
            val bw = new BufferedWriter(fw);
            bw.write(numVertices  + "\n" + numEdges + "\n")
            weightedGraph.foreach {case (x,y,z) => bw.write(x + " " + y +" "+ z + "\n")}
            bw.close()


          }
          

        }
}

