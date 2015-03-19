object CommonFunctions {

  case class DailyUserEventInfo(user_id:String,event_id:String,time_array: Array[Long],song_id_array :Array[String]) ;
  
  case class DailyUserEventDirectionInfo(user_id:String,sessionNum: Int,source_event_id: Int,destination_event_id: Int);
  
  val ALLJOBKEYLIST = Array("DailyUserJourney","DailyUserEventDirection") ;
  
  def getJobKey(givenKey:String): String = {
    for (key <- ALLJOBKEYLIST){
      if(key.toLowerCase().contains(givenKey.toLowerCase()))
      {
        return key;
      }
    }
    return "";
  }

  def getUserEventInfo(user_id:String,event_id:String,timeArray :Iterable[Long],itemArray : Iterable[String]) : Unit ={
    val song_id_array = itemArray.toArray;
    val sortedTimeList = timeArray.toList.sorted.toArray
    DailyUserEventInfo(user_id,event_id ,sortedTimeList,song_id_array)
  }
  
  def getUserEventDirectionInfo(user_id:String,timeEventArray : Iterable[(Long,String)]) : Unit = {
    val sortedTimeList = timeEventArray.toList.sortBy(_._1)
    
    
  }
  
}


