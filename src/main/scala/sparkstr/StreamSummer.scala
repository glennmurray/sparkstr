package sparkstr

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable


/**
 * Sum values in a stream, by key.
 */
object StreamSummer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: StreamSummer <master> <batchDuration>\n" +
        "For example: StreamSummer local[n] 1\n")
      return
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "StreamSummer", Seconds(batchDuration.toInt))//,
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new mutable.SynchronizedQueue[RDD[StreamSummerData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[StreamSummerData] = ssc.queueStream(rddQueue) 
    val mappedStream = inputStream.map(x => (x.label, x.value))
    mappedStream.print()
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    // Calculate and update the cumulative sums by key using 
    // PairDStreamFunctions.updateStateByKey[S](updateFunc: (Seq[V], Option[S]) ⇒ Option[S]),
    // which will work on the stream of k:v pairs in mappedStream.  This will yield
    // a state DStream[(String, Double)] which can be iteratively updated
    // with updateFunc.  Note that our updateFunc does not reference the pairs'
    // keys, but updateStateByKey keeps track of them and returns state by key.
    val updateFunc = (values: Seq[Double], state: Option[Double]) => {
      val currentSum:Double = values.sum // or foldLeft(0)(_ + _)
      val previousSum:Double = state.getOrElse(0)
      Some(previousSum + currentSum)
    }
    val stateDStream:DStream[(String, Double)] = reducedStream.updateStateByKey[Double](updateFunc)
    stateDStream.print()

    ssc.start()
    
    // Create and push some RDDs into the rddQueue.
    for (i <- 1 to 3) {
      val dataList = List(
        new StreamSummerData("LabelA", 1),   // LabelA sums to i.ii.
        new StreamSummerData("LabelA", 0.1),
        new StreamSummerData("LabelA", 0.01),

        new StreamSummerData("LabelB", i),   // LabelB sums to zero.
        new StreamSummerData("LabelB", -i))

      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(batchDuration.toInt * 999)
    }

    ssc.stop()
  }
}

class StreamSummerData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {}
