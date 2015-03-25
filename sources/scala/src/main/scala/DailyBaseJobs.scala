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
          val start_day = args(1)
          val end_day = args(2)
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
            val totalSummary = hive.hql(s"SELECT source_event_id,destination_event_id,sum(count) as count FROM  $eventTable GROUP BY source_event_id,destination_event_id ")
            val graphInfo = totalSummary.map { case Row(source_event_id: String, destination_event_id: String,count: Int) => (source_event_id,destination_event_id,count) }.collect
            val totalSum  = graphInfo.reduceLeft(_._3 + _._3);
            val weightedGraph = graphInfo.map{ case (x,y,z) => (x,y,z * -1.0/totalSum) }
            val numEdges = weightedGraph.length
            val numVertices = weightedGraph.flatMap{ case(a,b) => List(a,b)}.toSet.size
            File file = new File("/opt/bsb/sojourn/current/resources/totalSummary.txt");
            if (!file.exists()) {
              file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(str(numVertices) + "\n" + str(numEdges) + "\n")
            weightedGraph.foreach {case (x,y,z) => bw.write(x + " " + y +" "+ str(z) + "\n")}
            bw.close()

          }
          
          def updateDownloadSummary = {
            
          }
          

        }
}

