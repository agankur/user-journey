object CommonFunctions {

  case class DailyUserEventInfo(user_id:String,event_id:String,time_array: Array[Long],song_id_array :Array[String]) ;

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

  def getUserEventInfo(user_id:String,event_id:String,timeArray :Iterable[Long],itemArray : Iterable[String]) : DailyUserEventInfo ={
    val song_id_array = itemArray.toArray;
    val sortedTimeList = timeArray.toList.sorted.toArray
    DailyUserEventInfo(user_id,event_id ,sortedTimeList,song_id_array)
  }
  

  
}


