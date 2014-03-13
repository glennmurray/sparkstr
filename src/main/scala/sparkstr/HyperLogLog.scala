package sparkstr

import scala.annotation.tailrec
import scala.collection.mutable.{Buffer, SynchronizedQueue}
import scala.math._
import util.Random
import util.hashing.{MurmurHash3 => MH}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream


/**
 * Average values in a stream. 
 */
object HyperLogLog {
  /** The number of bins in the algorithm: can be 32 or 64 in this implementation.
    * More values can be implemented at the definition of alpha below. */
  val numBins = 64
  
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: HyperLogLog <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "HyperLogLog", Seconds(batchDuration.toInt), 
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to the stream.
    val rddQueue = new SynchronizedQueue[RDD[HyperLogLogData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[HyperLogLogData] = ssc.queueStream(rddQueue) 
    val mappedStream = inputStream.map(x => (x.label, x.value))
    //mappedStream.print()

    // Accumulate results and estimates using HllUpdateFunction.updateFunc in
    // updateStateByKey; this will yield a Dstream[(String, HyperLogLogState)].
    val stateDStream:DStream[(String, HyperLogLogState)]
      = mappedStream.updateStateByKey[HyperLogLogState](HllUpdateFunction.updateFunc)
    // Print some results.
    val stateDStreamValues = stateDStream.map(s => 
      s"${s._1} counter=${s._2.counter}, dvEstimate=${s._2.dvEstimate}, "
        +f"error: ${((s._2.dvEstimate.toDouble/s._2.counter)-1)*100}%.2f%%"
        +", bins: "+s._2.bins.mkString(" "))
    stateDStreamValues.print()

    ssc.start()
    
    // Create and stream some RDDs into the rddQueue.
    var counter:Long = 0
    for (i <- 0 to 99) {
      val n = 10000
      val dataList = for (j <- 1 to n) yield {
        //new HyperLogLogData(value = (n*i + j).toString)
        new HyperLogLogData(value = Random.nextInt.toString)
      }
      // makeRDD[T](seq: Seq[T], numSlices: Int): RDD[T]
      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList)
      Thread.sleep(1000)
    }

    Thread.sleep(1000)
    ssc.stop()
  }
}


class HyperLogLogData(val label:String = "HLL", val value:String = "")
  extends Serializable {}


class HyperLogLogState(val counter:Long = 0, 
                       val bins: Array[Int] = new Array[Int](HyperLogLog.numBins), 
                       val dvEstimate: Long = 0) 
 extends Serializable {}

/**
  * A container for updateFunc.
  */
object HllUpdateFunction {
  val updateFunc = (values: Seq[String], state: Option[HyperLogLogState]) => {
    val prevState = state.getOrElse(new HyperLogLogState())

    val newCounter = prevState.counter + values.length

    val bins: Array[Int] = prevState.bins
    val bins2Exp = (math.log(bins.length)/math.log(2)).toInt
    values.foreach(v => {
      val hash: Int = MH.stringHash(v)  // Has length <= 32.
      val bits: String = hash.toBinaryString
      val binIndex: Int = Integer.parseInt(bits.takeRight(bins2Exp), 2)

      val valWithZeros = bits.dropRight(bins2Exp) // 2^bins2Exp = # bins.
        @tailrec def countLeadingZeros(valWithZeros: String, acc: Int): Int = {
          if(valWithZeros.isEmpty || valWithZeros.head == '1') acc
          else countLeadingZeros(valWithZeros.tail, acc + 1)  // +1 for the first zero.
        }
      val numTrailingZeros = countLeadingZeros(valWithZeros.reverse, 0)
      bins(binIndex) = max(bins(binIndex), numTrailingZeros + 1)
    })
    
    val dvEstimate = calculateDvEstimate(bins)
    Some(new HyperLogLogState(newCounter, bins, dvEstimate))
  }

  def calculateDvEstimate(bins: Array[Int]): Int = {
    val m = bins.length
    val alpha = {if(m==32) 0.697  // See the Flajolet paper, Fig. 3.
    else 0.709}      // m=64 case
    val dvEst = alpha * m * m * (1 / bins.map(n => pow(2, -n)).sum)

    // Range corrections.  See Flajolet paper, Fig. 3.
    val dvEstimate = {
      if(dvEst <= 5*m/2) {
        val zeroBins = bins.filter(_==0).size
        if(zeroBins==0) dvEst
        else m*Math.log(m/zeroBins)
      } else if(dvEst <= (1/30) * Math.pow(2, 32)) dvEst
      else -Math.pow(2, 32) * Math.log(1 - dvEst/Math.pow(2, 32))
    }

    dvEstimate.toInt
  }
}
