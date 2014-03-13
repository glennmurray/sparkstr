package sparkstr

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.{Buffer, SynchronizedQueue}


/**
 * Average values in a stream. 
 */
object StreamLargest {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: StreamLargest <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "StreamLargest", Seconds(batchDuration.toInt), 
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new SynchronizedQueue[RDD[StreamLargestData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[StreamLargestData] = ssc.queueStream(rddQueue) 
    val mappedStream = inputStream.map(x => (x.label, x.value))
    //mappedStream.print()

    // Calculate the k largest values and update the state using updateFunc in
    // updateStateByKey; this will yield a Dstream[(String, StreamLargestState)].
    val updateFunc = (values: Seq[Double], state: Option[StreamLargestState]) => {
      val prevState = state.getOrElse(new StreamLargestState())
      val prevJSmallest:List[Double] = prevState.jSmallest
      val newJSmallest:List[Double] =  prevState.insertSmall(values.toList, prevJSmallest)
      val prevKLargest = prevState.kLargest
      val newKLargest = prevState.insertLarge(values.toList, prevKLargest)
      Some(new StreamLargestState(newJSmallest, newKLargest))
    }
    val stateDStream:DStream[(String, StreamLargestState)]
      = mappedStream.updateStateByKey[StreamLargestState](updateFunc)

    // Print some results.
    val stateDStreamValues = 
      stateDStream.map(s => s._1+" Smallest="+s._2.jSmallest+" Largest="+s._2.kLargest)
    stateDStreamValues.print()

    ssc.start()
    
    // Create and stream some RDDs into the rddQueue.
    for (i <- List(3, 2, 1, 2, 3, 4)) {
      val dataList = List(
        new StreamLargestData("labelA", i),
        new StreamLargestData("labelA", i/10.0),
        new StreamLargestData("labelB", i*2),
        new StreamLargestData("labelB", i/5.0)
      )

      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(1000)
    }

    ssc.stop()
  }
}


class StreamLargestData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {}

/**
 * Hold a list of the largest values; and a list of the smallest.
 * TODO parameterize the 3 and 4.
 */
class StreamLargestState(val jSmallest:List[Double] = List.fill(3)(1), 
                         val kLargest:List[Double] = List.fill(4)(0.0))
  extends Serializable {

  /**
   * Take the k largest from the concatenation of the two lists.
   * The result is not sorted. 
   */
  def insertLarge(insertable: List[Double], l: List[Double]): List[Double] = {
    val b: Buffer[Double] = l.toBuffer
    insertable.foreach(x => {
      val bMin = b.min
      if (x > bMin) {
        b -= bMin
        b += x
      }
    })
    b.toList
  }

  /**
   * Take the j smallest from the concatenation of the two lists.
   * The result is not sorted. 
   */
  def insertSmall(insertable: List[Double], l: List[Double]): List[Double] = {
    val b: Buffer[Double] = l.toBuffer
    insertable.foreach(x => {
      val bMax = b.max
      if (x < bMax) {
        b -= bMax
        b += x
      }
    })
    b.toList
  }
}
