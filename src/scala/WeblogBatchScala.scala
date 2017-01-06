package WeblogChallenge

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import com.github.nscala_time.time.Imports._
import org.apache.spark.streaming.{Durations, Minutes}

/**
  * Created by Robbie Yu (Bin Yu) on 04/01/2017.
  */
object WeblogBatchScala {
  def main(args: Array[String]): Unit = {

    //Initialize SparkConf and SparkContext
    val conf: SparkConf = new SparkConf().setAppName("WeblogBatchScala")
    val sc: SparkContext = new SparkContext(conf)
    val file: String = "/user/svc_customer360/clickStream/Weblog/2015_07_22_mktplace_shop_web_log_sample.log"

    //Initialize accumulator for total session count and total session time
    val sessionCounter: Accumulator[Long] = sc.accumulator[Long](0L)
    val sessionTimeAccumulator: Accumulator[Long] = sc.accumulator[Long](0L)

    //Read input file into actionRDD
    val actionRDD: RDD[String] = sc.textFile(file)

    //Parse input RDD[String] to RDD[ActionInfo]
    val parsedActionRDD: RDD[ActionInfo] = actionRDD.map(parseAction)

    //Group request by clientIP
    val ip2ActionsRDD: RDD[(String, Iterable[ActionInfo])] = parsedActionRDD.groupBy(_.clientIP)


    /**
      * For each clientIP group , flag each request's with a session group number
      * If time difference between each adjacent requests is longer than 15 Mins
      * Then the second request is treated as a start of a new session
      */
    val sessionizedActionRDD: RDD[(String, ActionInfo)] = ip2ActionsRDD.flatMapValues(iterable => sessionize(iterable.toSeq))

    //Group the sessionized RDD by clientIP_sessionFlag
    //clientIP_sessionFlag uniquely identify a session
    val ip2SessionizedActionRDD: RDD[(String, Iterable[(String, ActionInfo)])] = sessionizedActionRDD.groupBy(tuple => tuple._1+"_"+tuple._2.sessionFlag.toString)

    /**
      * 1. For each grouped session, traverse each of its requests and get
      *    the min timestamp as session start time and max timestamp as session end time
      * 2. Add each request's URL to Set for deduplication. The size of the Set is the uniq URL count of the session
      * 3. Calculate the session time using start time and end time
      * 4. Increment sessionCounter by 1 for each processed session
      * 5. Increment sessionTimeAccumulator by each session time
      * 6. Output SessionInfo object with (sessionID, sessionTime, uniqURLCount,startTime,endTime)
      * 7. Sort the output SessionInfo by its sessionTime in desc order
      */
    val sessionInfoRDD: RDD[SessionInfo] = ip2SessionizedActionRDD.map(tuple => {
      val sessionID = tuple._1
      val actionsInfoIterator = tuple._2
      var startTime: DateTime = null
      var endTime: DateTime = null

      //Initialize mutable Set for storing unique urls
      var uniqURL = scala.collection.mutable.Set[String]()

      //1. For each actionInfo per session(clientIP), calculate session's startTime, endTime
      //2. For each actionInfo per session(clientIP), add its requestURL to the above mutable Set for deduplication
      actionsInfoIterator.foreach(actionInfo => {
        val timestamp = actionInfo._2.timestamp
        val requestURL = actionInfo._2.requestURL
        val actionTime: DateTime = DateTime.parse(timestamp)
        if (Option(startTime).isEmpty) startTime = actionTime
        if (Option(endTime).isEmpty) endTime = actionTime
        if (actionTime.isBefore(startTime)) startTime = actionTime
        if (actionTime.isAfter(endTime)) endTime = actionTime
        uniqURL += requestURL
      })

      //Get the unique url count from the set
      val uniqURLCount: Int = uniqURL.size

      //Calculate sessionTime(session duration) using startTime and endTime in seconds
      val sessionTime: Long = (startTime to endTime).millis / 1000

      //Increment total session count accumulator by 1
      sessionCounter += 1L

      //Accumulate total session time
      sessionTimeAccumulator += sessionTime

      //Output calculated session information
      SessionInfo(sessionID, sessionTime, uniqURLCount,startTime,endTime )
    }).sortBy(_.sessionTime,false)

    //Evaluate sessionInfoRDD for it's accumulator
    evaluate(sessionInfoRDD)

    //Calculate average session time: total session time divided by total session count
    val avgSessionTime: Long = sessionTimeAccumulator.value/sessionCounter.value

    //Broadcast average session time for better performance
    val avgSessionTimeBroadcast = sc.broadcast(avgSessionTime)

    //Cache the RDD for reuse
    sessionInfoRDD.cache()

    //Print to console: session detail info: clientIP, session time, start time, end time, unique URL visits
    sessionInfoRDD.foreachPartition(iterator => {
      iterator.foreach(session => {
        println("User: " + session.sessionID.split("_")(0)
          + " started a " + session.sessionTime + " seconds session from "
          + session.startTime + " to " + session.endTime + " with " + session.uniqURLCount + " unique URL visits")
      })
    })

    //Print to console: average session time and the most engaged user IP and its session info
    sessionInfoRDD.take(1).foreach(session =>
      println("Average Session time is: " + avgSessionTimeBroadcast.value + " seconds! \n "
        + "The most engaged user is:"  + session.sessionID.split("_")(0)
        + " who started a " + session.sessionTime + " seconds session from "
        + session.startTime + " to " + session.endTime + " with " + session.uniqURLCount + " unique URL visits")
    )

    sc.stop()
  }

  //Parse input RDD to ActionInfo object for easy analysis
  def parseAction(action:String) = ActionInfo(action.split(" ")(2).split(":")(0),action.split(" ")(0),action.split(" ")(12),0)

  //Calculate sessionFlag for each request from the same clientIP.
  //If time difference between two adjacent requests is longer than 15 Mins
  //Then the second request is treated as a start of a new session and sessionFlag plus one
  def sessionize(lines: Seq[ActionInfo]) = {
    if(lines.size < 2) {
      lines
    } else {
      val sorted = lines.sortBy(_.timestamp)
      sorted.tail.scanLeft(sorted.head) { case (prev, curr) =>
        // Gets the correct session session
        curr.sessionFlag = if (sessionTimedOut) prev.sessionFlag + 1 else prev.sessionFlag.toInt
        // Returns true if the session has timed out between the prev and cur LogLine
        def sessionTimedOut = (DateTime.parse(prev.timestamp) to DateTime.parse(curr.timestamp)).millis >  Minutes(15).milliseconds

        curr
      }
    }
  }

  //Trigger the accumulator operation
  def evaluate[T](rdd:RDD[T]) = {
    rdd.sparkContext.runJob(rdd,(iter: Iterator[T]) => {
      while(iter.hasNext) iter.next()
    })
  }

}

//Case class for request information encapsulation
case class ActionInfo(clientIP:String, timestamp:String, requestURL:String, var sessionFlag: Int) extends Serializable

//Case class for session information encapsulation
case class SessionInfo(sessionID:String, sessionTime:Long, uniqURLCount:Int,startTime: DateTime,endTime: DateTime) extends Serializable
