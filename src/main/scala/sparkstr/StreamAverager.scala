package sparkstr

import scala.collection.mutable.SynchronizedQueue

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream


/**
 * Average values in a stream. 
 */
object StreamAverager {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: StreamAverager <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "StreamAverager", Seconds(batchDuration.toInt), 
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new SynchronizedQueue[RDD[StreamAveragerData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[StreamAveragerData] = ssc.queueStream(rddQueue) 
    val mappedStream:DStream[(String, Double)] = inputStream.map(x => (x.label, x.value))
    //mappedStream.print()

    // Calculate and update the cumulative average using 
    // PairDStreamFunctions.updateStateByKey[S](updateFunc: (Seq[V], Option[S]) ⇒ Option[S]),
    // which will work on the stream of k:v pairs in mappedStream.  This will yield a state 
    // Dstream[(String, StreamAveragerState)] which can be iteratively updated
    // with updateFunc.  Note that our updateFunc does not reference the pairs's keys,
    // but updateStateByKey keeps track of them and returns state by key.
    val updateFunc = (values: Seq[Double], state: Option[StreamAveragerState]) => {
      val prevState = state.getOrElse(new StreamAveragerState())
      // The newAvg is (n/(n+k))*prevAvg) + (k/(n+k))*currAvg, simplified.
      val newCount:Long = prevState.count + values.size
      val newAvg = (prevState.average * prevState.count + values.sum)/newCount
      Some(new StreamAveragerState(newCount, newAvg))
    }
    val stateDStream:DStream[(String, StreamAveragerState)]
      = mappedStream.updateStateByKey[StreamAveragerState](updateFunc)
    //stateDStream.print()

    // Print some state values.
    val stateDStreamValues 
      = stateDStream.map(s => (s._1, " count="+s._2.count, " avg="+s._2.average))
    stateDStreamValues.print()

    ssc.start()
    
    // Create and stream some RDDs into the rddQueue.
    StreamAveragerDataGenerator.pushToRdd(ssc, rddQueue, 1000)

    ssc.stop()
  }
}


class StreamAveragerData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {
  override def toString():String = {label+s", value=$value"}
}


class StreamAveragerState(val count:Long = 0, 
                          val average:Double = 0.0)
    extends Serializable {}


object StreamAveragerDataGenerator {
  def pushToRdd(ssc: StreamingContext, 
                rddQueue:SynchronizedQueue[RDD[StreamAveragerData]],
                pause:Int): Unit = {
    for (i <- 1 to 7) {
      val dataList = List(
        new StreamAveragerData("labelA", i), // For each labelA count c, let n=c/4.
        new StreamAveragerData("labelA", i), //   avg = (4 * n*(n+1)/2) / c
        new StreamAveragerData("labelA", i), //       = 2 * n*(n+1) / c
        new StreamAveragerData("labelA", i),
 
        new StreamAveragerData("labelB", i),
        new StreamAveragerData("labelB", -i)  // Label B should avg to zero.
      )

      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(pause)
    }
  }
}
