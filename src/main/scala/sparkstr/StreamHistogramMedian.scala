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
object StreamHistogramMedian {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: StreamHistogramMedian <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "HistogramMedian", Seconds(batchDuration.toInt), 
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new SynchronizedQueue[RDD[HistogramMedianData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[HistogramMedianData] = ssc.queueStream(rddQueue) 
    val mappedStream:DStream[(String, Double)] = inputStream.map(x => (x.label, x.value))
    //mappedStream.print()

    // Calculate and update the histogram using updateStateByKey;
    // this will yield a state Dstream[(String, HistogramMedianState)].
    val stateDStream:DStream[(String, HistogramMedianState)]
      = mappedStream.updateStateByKey[HistogramMedianState](HistogramMedianUpdateFunction.updateFunc)
    //stateDStream.print()
    val stateDStreamValues = stateDStream.map(s => s.toString)
    stateDStreamValues.print()

    ssc.start()

    // Create and stream some RDDs into the rddQueue.
    HistogramMedianDataGenerator.pushToRdd(ssc, rddQueue, 1000)

    ssc.stop()
  }
}


class HistogramMedianData(val label:String = "label", val value:Double = 0.0) 
  extends Serializable {
  override def toString():String = {label+s", value=$value"}
}

class HistogramMedianState(val count:Long = 0, 
                           val mean:Double = 0.0,  // For comparison to the median.
                           val histogram:Array[Long] = new Array[Long](20), // # bins.
                           val median:Double = 0.0)
    extends Serializable {
  val numberOfBins = histogram.length
  val min = 0.0
  val max = 100.0
  val increment = (max - min) / numberOfBins
  val binEndPts: Array[Double] = (1 to numberOfBins).map(min + _*increment).toArray
  // Because <= may fail for the last binEndPt due to rounding:
  binEndPts(numberOfBins - 1) = max

  override def toString():String = f" count=$count, mean=$mean%.4f, median=${median}%.4f, "+
    "histogram: "+histogram.mkString(" ") 
}


/** 
 * A container for the DStream.updateStateByKey updateFunc.
 */
object HistogramMedianUpdateFunction {
  /**
   * @param values The new data.
   * @param state The previous state from the last RDD iteration.
   * @return The new state: created from the previous state by updating it 
   * with the new data.
   */
  val updateFunc:(Seq[Double], Option[HistogramMedianState]) => Option[HistogramMedianState] =
    (values: Seq[Double], state: Option[HistogramMedianState]) => {
      if(values.length==0) {
        state
      } else {
        val prevState = state.getOrElse(new HistogramMedianState())
        val prevCount = prevState.count
        val prevMedian = prevState.median
        val prevHistogram: Array[Long] = prevState.histogram
        val binEnds: Array[Double] = prevState.binEndPts

        // Generate the values's histogram.
        val valuesHistogram = findHistogram(values, binEnds)

        val updatedCount = prevState.count + values.size
        val updatedMean = (prevState.mean * prevState.count + values.sum)/updatedCount
        val updatedHistogram = prevHistogram.zip(valuesHistogram).map(x => x._1 + x._2)
        val updatedMedian = findHistogramMedian(updatedHistogram, binEnds, updatedCount/2)

        Some(new HistogramMedianState(updatedCount, updatedMean,
                                      updatedHistogram, updatedMedian))
    }
  }

  /**
   * See http://stackoverflow.com/a/4663557/29771
   */
  def findHistogram(values:Traversable[Double], binEndPts: Array[Double]): Array[Long] = {
    //val bins = new Array[Long](binEndPts.size - 1)
    val bins = new Array[Long](binEndPts.size - 1)

    // Return the bin index for a value.
    def binIndex(d: Double):Int = binEndPts.indexWhere(d < _) - 1

    // Compute how many in each bin
    values foreach { d => bins(binIndex(d)) += 1 }
    bins
  }

  /**
   * This approximates the median.  This may be a very
   * poor approximation for small numbers of values or
   * values that are not continuously distributed.
   */
  def findHistogramMedian(bins:Array[Long], binEnds:Array[Double], 
                          medianIndex:Long): Double = {

    // Make the bins cumulative, left to right.
    val partialSums = bins.scanLeft(0L)(_+_).drop(1)
    // The median is in this bin.
    val medianBin:Int = partialSums.indexWhere(medianIndex < _)

    // We'll just take the  average of the endpoints of this bin.
    (binEnds(medianBin) + binEnds(medianBin + 1))/2
  }
}


/**
 * Generate some data for the histogram.
 */
object HistogramMedianDataGenerator {
  def pushToRdd(ssc: StreamingContext,
    rddQueue: SynchronizedQueue[RDD[HistogramMedianData]],
    pause: Int): Unit = {
    var numIters = 9
    for (i <- 1 to numIters) {
      
      var maxCount = 5
      val abLeft: Array[HistogramMedianData] = new Array(maxCount)
      val abRight: Array[HistogramMedianData] = new Array(maxCount)

      // Skew left (skew positive) 
      var count = 0
      while (count < maxCount) {
        val v = Random.nextDouble * 100
        abLeft(count) = new HistogramMedianData("SkewLeft", v)
        count += 1
        if (v >= 50 && count < maxCount) {
          abLeft(count) = new HistogramMedianData("SkewLeft", v/4)
          count += 1
        }
      }
      // Skew right (skew negative)
      count = 0
      while (count < maxCount) {
        val v = Random.nextDouble * 100
        abRight(count) = new HistogramMedianData("SkewRight", v)
        count += 1
        if (v < 50 && v >= 25 && count < maxCount) {
          abRight(count) = new HistogramMedianData("SkewRight", 2*v)
          count += 1
        }
      }

      val dataList:List[HistogramMedianData] = abLeft.toList ++ abRight.toList
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(pause)
    }
  }
}
