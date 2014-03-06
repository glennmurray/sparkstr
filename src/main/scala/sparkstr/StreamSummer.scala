package sparkstr

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import scala.collection.mutable.SynchronizedQueue


/**
 * Sum values in a stream.
 *
 * To run this on your local machine with sbt:
 *    'run local[2] 3'
 */
object StreamSummer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: StreamSummer <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    val updateFunc = (values: Seq[Double], state: Option[Double]) => {
      val currentCount:Double = values.sum//foldLeft(0)(_ + _)
      val previousCount:Double = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "StreamSummer", Seconds(batchDuration.toInt),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new SynchronizedQueue[RDD[StreamSummerData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[StreamSummerData] = ssc.queueStream(rddQueue) 
    val mappedStream = inputStream.map(x => (x.label, x.value))
    mappedStream.print()
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    // Update the cumulative count using updateStateByKey;
    // this will give a Dstream made of state (cumulative sums).
    val stateDStream:DStream[(String, Double)] 
      = reducedStream.updateStateByKey[Double](updateFunc)
    stateDStream.print()

    ssc.start()
    
    // Create and push some RDDs into the rddQueue.
    for (i <- 1 to 7) {
      val dataList = List(
        new StreamSummerData("labelA", i),
        new StreamSummerData("labelA", i + i*(0.01)),
        new StreamSummerData("labelB", -i),
        new StreamSummerData("labelB", -i - i*(0.01)))

      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList, 3)
      Thread.sleep(1000)
    }
    ssc.stop()
    System.exit(0)
  }
}

class StreamSummerData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {}
