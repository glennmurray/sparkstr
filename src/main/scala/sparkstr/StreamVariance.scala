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
      = mappedStream.updateStateByKey[StreamVarianceState](StreamVarianceUpdateFunction.updateFunc)
    //stateDStream.print()
    val stateDStreamValues = stateDStream.map(s => s.toString)
    stateDStreamValues.print()

    ssc.start()

    // Create and stream some RDDs into the rddQueue.
    StreamVarianceDataGenerator.pushToRdd(ssc, rddQueue, 1000)

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
  override def toString():String = f"count=$count, mean=${mean}%.4f, "+
    f"sumSquaresVar=$sumSquaresVar%.4f, welfordVar=$welfordVar%.4f"
}


/** 
 * A container for the DStream.updateStateByKey updateFunc.
 */
object StreamVarianceUpdateFunction {
  /**
   * @param values The new data.
   * @param state The previous state from the last RDD iteration.
   * @return The new state: created from the previous state by updating it 
   * with the new data.
   */
  val updateFunc:(Seq[Double], Option[StreamVarianceState]) => Option[StreamVarianceState] =
    (values: Seq[Double], state: Option[StreamVarianceState]) => {
      if(values.length==0) {
        state
      } else {
        val prevState = state.getOrElse(new StreamVarianceState())
        val prevCount = prevState.count
        val prevMean = prevState.mean
        val prevSumSquaresVar = prevState.sumSquaresVar
        val prevWelfordVar = prevState.welfordVar

        val valsCount = values.length
        val valsMean = values.sum / valsCount

        // Generate the new running statistics.
        val count:Long = prevState.count + values.size
        val mean = (prevState.mean * prevState.count + values.sum) / count

        val valsSumSquaresVariance = sumSquaresVariance(values)
        val sumSquaresVar = combinePrevCurrVars(prevSumSquaresVar, prevMean, prevCount,
                                                valsSumSquaresVariance, valsMean, valsCount)

        val valsWelfordVariance = welfordVariance(values)
        val welfordVar = combinePrevCurrVars(prevWelfordVar, prevMean, prevCount,
                                             valsWelfordVariance, valsMean, valsCount)

        Some(new StreamVarianceState(count,
                                     mean,
                                     sumSquaresVar, 
                                     welfordVar))
    }
  }

  /**
   * Use "sum of squares minus mean squared" to find the variance.
   */
  def sumSquaresVariance(values: Seq[Double]): Double = {
    val k:Int = values.size
    val valuesMean:Double = values.sum/values.length
    (values.map(x => x*x).sum / k) - valuesMean * valuesMean
  }

  /**
   * Use "Welford's Algorithm" to find the variance.  We iterate through a batch
   * of values per RDD, even though the algorithm could be done as a streaming
   * algorithm if we did one Welford iteration per each Spark Streaming
   * iteration; i.e., if each RDD had only one x_i data value.
   * @return The variance of the values.
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
   * Given the variances, means, and counts from two sets of data, find the
   * variance of the combined set.  This algorithm is equivalent to the algorithm
   * given in Wikipedia's "Algorithms for calculating variance", crediting Chan.
   * Another equivalent implementation is in spark.util.StatCounter.
   */
  def combinePrevCurrVars(prevVar:Double, prevMean:Double, prevCount:Long,
                          currVar:Double, currMean:Double, currCount:Long): Double = {
    // The proportion
    val p:Double = prevCount.toDouble/(prevCount + currCount)
    p*prevVar + (1-p)*currVar + p*(1-p)*(prevMean-currMean)*(prevMean-currMean)
  }
}


object StreamVarianceDataGenerator {
  def pushToRdd(ssc: StreamingContext, 
                rddQueue:SynchronizedQueue[RDD[StreamVarianceData]], 
                pause:Int): Unit = {
    for (i <- 0 to 9) {
      val sineCount = 99
      val dataListSine:List[StreamVarianceData] = {
        for(j <- 0 to sineCount) 
        yield {new StreamVarianceData("sine", Math.sin(2*Math.PI * Random.nextDouble))}
      }.toList

      val dataListA:List[StreamVarianceData] = List(
        // LabelA should avg to zero and for i=k have variance = sum(i^2 + (-i)^2)/2k.
        new StreamVarianceData("LabelA", i),  
        new StreamVarianceData("LabelA", -i) 
      )

      val dataList:List[StreamVarianceData] = dataListA ++ dataListSine // :::
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(pause)
    }
  }
}
