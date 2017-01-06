# WeblogChallenge Solution

##Step One: Initialization

###1. Initialize accumulator for total session count and total session time
```scala
val sessionCounter: Accumulator[Long] = sc.accumulator[Long](0L)
val sessionTimeAccumulator: Accumulator[Long] = sc.accumulator[Long](0L)
```
###2. Define Case class for request info and Session info encapsulation
```scala
case class ActionInfo(clientIP:String, timestamp:String, requestURL:String, var sessionFlag: Int) extends Serializable
case class SessionInfo(sessionID:String, sessionTime:Long, uniqURLCount:Int,startTime: DateTime,endTime: DateTime) extends Serializable
```

##Step Two: Define util functions

###1. parseAction: Parse input RDD to ActionInfo object for easy analysis
```scala
def parseAction(action:String) = ActionInfo(action.split(" ")(2).split(":")(0),action.split(" ")(0),action.split(" ")(12),0)
```

###2. sessionize: Calculate sessionFlag for each request from the same clientIP.
If time difference between two adjacent requests is longer than 15 Mins
Then the second request is treated as a start of a new session and sessionFlag plus one
```scala
def sessionize(actions: Seq[ActionInfo]) = {
    if(actions.size < 2) actions else {
      val sorted: Seq[ActionInfo] = actions.sortBy(_.timestamp)
      sorted.tail.scanLeft(sorted.head) { case (prevAction, currAction) =>
        // Returns true if the session has timed out between the prevAction and currAction
        def sessionTimedOut = (DateTime.parse(prevAction.timestamp) to DateTime.parse(currAction.timestamp)).millis >  Minutes(15).milliseconds
        // Set currAction sessionFlag
        currAction.sessionFlag = if (sessionTimedOut) prevAction.sessionFlag + 1 else prevAction.sessionFlag
        currAction
      }
    }
  }
```

###3. evaluate: Evaluate RDD for its Accumulator side effect
```scala
def evaluate[T](rdd:RDD[T]) = { rdd.sparkContext.runJob(rdd,(iter: Iterator[T]) =>  while(iter.hasNext) iter.next()) }
```

##Step Three: Process batch data
###1. Read input Weblog data from file and map to ActionInfo Object using parseAction function
*  RDD[String] => RDD[ActionInfo]
```scala
val actionRDD: RDD[String] = sc.textFile(file)
val parsedActionRDD: RDD[ActionInfo] = actionRDD.map(parseAction)
```

###2. Group request by clientIP 
*  RDD[ActionInfo] => RDD[(String, Iterable[ActionInfo])]
```scala
val ip2ActionsRDD: RDD[(String, Iterable[ActionInfo])] = parsedActionRDD.groupBy(_.clientIP)
```

###3. Within each clientIP group , flag each request with a session group number
*  RDD[(String, Iterable[ActionInfo])] => RDD[(String, ActionInfo)] with ActionInfo's sessionFlag set
```scala
val sessionizedActionRDD: RDD[(String, ActionInfo)] = ip2ActionsRDD.flatMapValues(iterable => sessionize(iterable.toSeq))
```

###4. Group the sessionized RDD by clientIP_sessionFlag
* Each clientIP could have multiple sessions, clientIP_sessionFlag uniquely identify a session
* RDD[(String, ActionInfo)] => RDD[(String, Iterable[(String, ActionInfo)])] with ActionInfo's sessionFlag set
```scala
val ip2SessionizedActionRDD: RDD[(String, Iterable[(String, ActionInfo)])] = sessionizedActionRDD.groupBy(tuple => tuple._1+"_"+tuple._2.sessionFlag.toString)
```

###5. For each grouped session, traverse each of its requests and get the min timestamp as session start time and max timestamp as session end time
*  Add each request's URL to Set for deduplication. The size of the Set is the uniq URL count of the session
*  Calculate the session time using start time and end time
*  Increment sessionCounter by 1 for each processed session
*  Increment sessionTimeAccumulator by each session time
*  Output SessionInfo object with (sessionID, sessionTime, uniqURLCount,startTime,endTime)
*  Sort the output SessionInfo by its sessionTime in desc order
*  RDD[(String, Iterable[(String, ActionInfo)])] => RDD[SessionInfo]
```scala
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
```

###5. Calculate average session time
* Total session time divided by total session count
* Broadcast average session time for better performance
```scala
val avgSessionTime: Long = sessionTimeAccumulator.value/sessionCounter.value
val avgSessionTimeBroadcast = sc.broadcast(avgSessionTime)
```

###6.Print analyzed output to console
Session detail info: clientIP, session time, start time, end time, unique URL visits
```scala
sessionInfoRDD.foreachPartition(iterator => {
      iterator.foreach(session => {
        println("User: " + session.sessionID.split("_")(0)
          + " started a " + session.sessionTime + " seconds session from "
          + session.startTime + " to " + session.endTime + " with " + session.uniqURLCount + " unique URL visits")
      })
    })
```
Aaverage session time and the most engaged user IP and its session info
```scala
sessionInfoRDD.take(1).foreach(session =>
      println("Average Session time is: " + avgSessionTimeBroadcast.value + " seconds! \n "
        + "The most engaged user is:"  + session.sessionID.split("_")(0)
        + " who started a " + session.sessionTime + " seconds session from "
        + session.startTime + " to " + session.endTime + " with " + session.uniqURLCount + " unique URL visits")
    )
```

##Sample Output

| User           | SessionTime(inSeconds)                    | StartTime           | EndTime           |Uniq URL Visits          |
| -------------- |:-----:| :-----------------------:| :---------------------: | --:|
| 141.228.250.141 | 1656 | 2015-07-22T10:34:25.282Z | 2015-07-22T11:02:01.575 | 54 |
| 113.19.86.10 | 54 | 2015-07-22T06:55:16.983Z | 2015-07-22T06:56:11.154Z | 2 |
| 164.139.7.150 | 2 | 2015-07-22T05:14:55.610Z | 2015-07-22T05:14:58.437Z | 2 |
| 14.195.155.233 | 54 | 2015-07-22T10:36:33.867Z | 2015-07-22T10:37:28.129Z | 4 |
| 123.63.235.34 | 1656 | 2015-07-22T10:34:50.996Z | 2015-07-22T11:02:27.115Z | 3 |
| 14.98.120.185 | 54 | 2015-07-22T16:23:47.970Z | 2015-07-22T16:24:42.354Z | 2 |
| 14.98.202.201 | 2 | 2015-07-22T09:00:30.820Z | 2015-07-22T09:00:32.982Z | 2
| 14.97.43.194 | 54 | 2015-07-22T18:04:16.938Z | 2015-07-22T18:05:10.965Z | 6
| 14.140.116.145 | 1656 | 2015-07-22T10:35:35.375Z | 2015-07-22T11:03:11.558Z | 17 |
| 122.177.193.80 | 54 | 2015-07-22T09:00:58.123Z | 2015-07-22T09:01:52.963Z | 6 |
| 103.26.225.229 | 2 | 2015-07-22T17:43:43.527Z | 2015-07-22T17:43:45.634Z | 2 |
| 117.217.172.105 | 54 | 2015-07-22T16:13:23.552Z | 2015-07-22T16:14:17.851Z | 2 |

Average Session time in seconds: 100  

The most engaged user:52.74.219.71 | 
SessionTime(in Seconds): 2069 | 
StartTime: 2015-07-22T10:30:28.220Z | 
EndTime:2015-07-22T11:04:57.382Z | 
Unique URL visits: 9532
