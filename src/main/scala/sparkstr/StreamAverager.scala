package sparkstr

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import scala.collection.mutable.SynchronizedQueue


/**
 * Average values in a stream. 
 *
 * To run this on your local machine with sbt:
 *    'run local[2] 1'
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
    val mappedStream = inputStream.map(x => (x.label, x.value))
    //mappedStream.print()
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //reducedStream.print()

    // Update the cumulative count using updateStateByKey;
    // this will give a Dstream made of state (cumulative sums).
    val updateFunc = (values: Seq[Double], state: Option[StreamAveragerState]) => {
      val prevState = state.getOrElse(new StreamAveragerState())
      val newCount:Long = prevState.count + values.size
      val newAvg = (prevState.average * prevState.count + values.sum)/newCount
      Some(new StreamAveragerState(newCount, newAvg))
    }
    val stateDStream:DStream[(String, StreamAveragerState)]
      = reducedStream.updateStateByKey[StreamAveragerState](updateFunc)
    //stateDStream.print()
    val stateDStreamValues = stateDStream.map(s => (s._1, s._2.count, s._2.average))
    stateDStreamValues.print()

    ssc.start()
    
    // Create and stream some RDDs into the rddQueue.
    for (i <- 1 to 7) {
      val dataList = List(
        new StreamAveragerData("labelA", i),
        new StreamAveragerData("labelA", i), // Should avg to count+1.
        new StreamAveragerData("labelB", i),
        new StreamAveragerData("labelB", -i))  // Label B should avg to zero.

      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList, 3)
      Thread.sleep(1000)
    }
    ssc.stop()
    System.exit(0)
  }
}

class StreamAveragerData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {}

class StreamAveragerState(val count:Long = 0, val average:Double = 0.0)
  extends Serializable {}