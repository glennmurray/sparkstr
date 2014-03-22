package sparkstr

import scala.annotation.tailrec
import scala.collection.mutable.SynchronizedQueue
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream


/**
 * Find variances for values in a stream. 
 */
object StreamVariance {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: StreamVariance <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "StreamVariance", Seconds(batchDuration.toInt), 
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new SynchronizedQueue[RDD[StreamVarianceData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[StreamVarianceData] = ssc.queueStream(rddQueue) 
    val mappedStream:DStream[(String, Double)] = inputStream.map(x => (x.label, x.value))
    //mappedStream.print()

    //val mappedStreamA:DStream[(String, Double)] = mappedStream.filter(x => x._1=="labelA")
    //mappedStreamA.print()

    // Calculate and update the cumulative variances using updateStateByKey;
    // this will yield a state Dstream[(String, StreamVarianceState)].
    val stateDStream:DStream[(String, StreamVarianceState)]
      = mappedStream.updateStateByKey[StreamVarianceState](UpdateFunction.updateFunc)
    //stateDStream.print()
    val stateDStreamValues = stateDStream.map(s => s.toString)
    stateDStreamValues.print()

    ssc.start()

    // Create and stream some RDDs into the rddQueue.
    DataGenerator.pushToRdd(ssc, rddQueue, 1000)

    ssc.stop()
  }
}


class StreamVarianceData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {
  override def toString():String = {label+s", value=$value"}
}


class StreamVarianceState(val count:Long = 0, 
                          val mean:Double = 0.0,
                          val sumSquaresVar:Double = 0.0,
                          val welfordVar:Double = 0.0)
    extends Serializable {
  override def toString():String = f"count=$count, mean=${mean}%.4f, sumSquaresVar=$sumSquaresVar%.4f  "
}

/** 
 * A container for the DStream.updateStateByKey updateFunc.
 */
object UpdateFunction {
  val updateFunc = (values: Seq[Double], state: Option[StreamVarianceState]) => {
    if(values.length==0) {
       state
    } else {
      val prevState = state.getOrElse(new StreamVarianceState())
      val newCount:Long = prevState.count + values.size

      // Generate the new statistics.
      val totalMean = (prevState.mean * prevState.count + values.sum)/newCount
      val newSumSquaresVariance =
        sumSquaresVariance(prevState.sumSquaresVar, prevState.mean, prevState.count, values)
      Some(new StreamVarianceState(prevState.count+values.size, 
                                   totalMean,
                                   newSumSquaresVariance, 0))  // TODO: Welford
    }
  }

  /**
   * Use "sum of squares minus mean squared" to find the variance.
   */
  def sumSquaresVariance(prevVar:Double = 0.0, prevMean:Double = 0.0, prevCount:Long = 0, 
                         values: Seq[Double]): Double = {
    val k:Int = values.size
    val valuesMean:Double = values.sum/values.length
    val valuesVar = (values.map(x => x*x).sum / k) - valuesMean * valuesMean
    combinePrevCurrVars(prevVar, prevMean, prevCount, valuesVar, valuesMean, k)
  }

  /**
   * Use "Welford's Algorithm" to find the variance.  The "Batch" in the name
   * is there because we iterate through a batch of values per RDD, even though
   * the algorithm could be done as a streaming algorithm if we did one
   * Welford iteration step per each Spark Streaming iteration step; i.e., if 
   * each RDD had only one x_i data value.
   */
  def welfordVariance(values: Seq[Double]): Double = { 
    @tailrec 
    def welfordRec(values: Seq[Double], m2:Double, mean:Double, n:Int): Double = {
      if(values.isEmpty) m2 / (n-1) // Note we need n-1 for pop. variance here.
      else {
        val x = values.head
        val delta = x - mean
        val newMean = mean + delta/n
        val newM2 = m2 + delta*(x - newMean)
        welfordRec(values.tail, newM2, newMean, n+1)
      }
    }
    welfordRec(values, 0.0, 0.0, 1)
  }

  /**
   * Wikipedia "Algorithms for calculating variance", Chan.
   * An equivalent implementation is in spark.util.StatCounter.
   */
  def combinePrevCurrVars(prevVar:Double, prevMean:Double, prevCount:Long,
                          currVar:Double, currMean:Double, currCount:Long): Double = {
    // The proportion
    val p:Double = prevCount.toDouble/(prevCount + currCount)
    p*prevVar + (1-p)*currVar + p*(1-p)*(prevMean-currMean)*(prevMean-currMean)
  }
}


object DataGenerator {
  def pushToRdd(ssc: StreamingContext, 
                rddQueue:SynchronizedQueue[RDD[StreamVarianceData]], pause:Int): Unit = {
    for (i <- 0 to 99) {
      val sineCount = 99
      val dataListSine:List[StreamVarianceData] = {
        for(j <- 0 to sineCount) 
        yield {new StreamVarianceData("sine", Math.sin(2*Math.PI * Random.nextDouble))}
      }.toList

      val dataListB:List[StreamVarianceData] = List(
        new StreamVarianceData("LabelB", i),  // Label B should avg to zero and
        new StreamVarianceData("LabelB", -i)  // have variance = sum(i^2)/(i+1).
      )

      val dataList:List[StreamVarianceData] = dataListSine //::: dataListSine // :::
      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(pause)
    }
  }
}
