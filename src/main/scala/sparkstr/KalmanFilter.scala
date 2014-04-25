package sparkstr

import scala.annotation.tailrec
import scala.collection.mutable.SynchronizedQueue
import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import breeze.linalg._

/**
 * Find variances for values in a stream. 
 */
object KalmanFilter {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("\nUsage: KalmanFilter <master> <batchDuration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1.\n")
      System.exit(1)
    }
    val Array(master, batchDuration) = args

    // Create the context with the given batchDuration.
    val ssc = new StreamingContext(master, "KalmanFilter", Seconds(batchDuration.toInt), 
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("./output")

    // Create the queue through which RDDs can be pushed to a QueueInputDStream.
    val rddQueue = new SynchronizedQueue[RDD[KalmanFilterData]]()
    
    // "Create an input stream from a queue of RDDs."
    val inputStream:DStream[KalmanFilterData] = ssc.queueStream(rddQueue) 
    val mappedStream:DStream[(String, KalmanFilterData)] = inputStream.map(x => (x.label, x))
    //mappedStream.print()

    // Calculate and update the cumulative variances using updateStateByKey;
    // this will yield a state Dstream[(String, KalmanFilterState)].
    val stateDStream:DStream[(String, KalmanFilterState)]
      = mappedStream.updateStateByKey(KalmanFilterUpdateFunction.updateFunc)
    stateDStream.print()
    val stateDStreamValues = stateDStream.map(s => s._2.toString)
    //stateDStreamValues.print()

    ssc.start()

    // Create and stream some RDDs into the rddQueue.
    KalmanFilterDataGenerator.pushToRdd(ssc, rddQueue, 1000)

    ssc.stop()
  }
}


object KalmanFilterConstants {
  val deltaT = 1.0               // Time interval.
  val F = DenseMatrix((1.0, deltaT), (0.0, 1.0)) // State transition model.
  val a = DenseVector(3.0, 3.0)                  // Acceleration.
  val g = DenseVector(deltaT*deltaT/2, deltaT) // Newton: x_k = F*x_{k-1} + G*a.
  val GA = (g :* a)              // Convenience

  val sigmaM = 1.0               // Variance of the measurement noise.
  val Q = g * g.t * sigmaM       // Observation noise covariance estimate, scalar.
  val vN = 1.0                   // Measurement noise, normal with variance R.
  val r = 0.5;                   // Measurement noise covariance estimate.
  val H = DenseMatrix(1.0, 0.0).t  // Observation matrix (row).
}


class KalmanFilterState(val count:Long = 0, 
                        // Initializing the actual distance/velocity values
                        val x:DenseVector[Double] = DenseVector(30.0, -9.0),
                        // Initializing the estimated distance/velocity values
                        val xe:DenseVector[Double] = DenseVector(10.0, 0.0),
                        // Initial (a posteriori) error covariance estimate.
                        val p:DenseMatrix[Double] = DenseMatrix((2.0, 0.0), (0.0, 2.0))
                        )
    extends Serializable {

  override def toString():String = {
    def formatDV(v:DenseVector[Double]): String = {
      "[ " + v.map(x => "%.4f".format(x)).toArray.mkString(", ") + " ]"
    }
    def formatP(p:DenseMatrix[Double]): String = {
      "[" + formatDV(p(0,::).t) +","+ formatDV(p(1,::).t) +"]"
    }
    s" x=${formatDV(x)}, xe=${formatDV(xe)}, p=${formatP(p)}"
  }
}


/** 
 * A container for the DStream.updateStateByKey updateFunc.
 */
object KalmanFilterUpdateFunction {

  val updateFunc = (values: Seq[KalmanFilterData], state: Option[KalmanFilterState]) => {
    if(values.length==0) {
      state
    } else {
      val x:DenseVector[Double] = values(0).actualState
      val z:DenseVector[Double] = values(0).observedState // Includes noise.
      val prevState = state.getOrElse(new KalmanFilterState())
      val newCount:Long = prevState.count + values.size

      // Generate the new statistics.
      val F:DenseMatrix[Double] = KalmanFilterConstants.F   // State transition matrix.
      val GA:DenseVector[Double] = KalmanFilterConstants.GA
      val H:DenseMatrix[Double] = KalmanFilterConstants.H
      val iD = DenseMatrix.eye[Double](2) // 2x2 identity matrix.

      // Prediction Step. 
      val xe = F * prevState.xe + GA  // Predicted position and speed, 2x1.
      // Prediction of the error covariance, 2x2.
      val p = F * prevState.p * F.t + KalmanFilterConstants.Q

      // Update Step.
      val y = z - (H * xe)   // Innovation, a scalar, diff: actual-prediction.
      val s = H*p*H.t + KalmanFilterConstants.r      // Covariance of innovation, a scalar.
      val k = p * H.t * inv(s) // Kalman gain, as time incr., weights prediction over obs.
      val upXe = xe + (k*y)        // Updated 2x1 state estimate.
      val upP = (iD - (k*H))*p     // New calculated error cov after observation of z.


      Some(new KalmanFilterState(newCount, x, upXe, upP))
    }
  }
}


/**
 * A container for the incoming data. 
 * @param A label.
 * @param actualState The actual or true position and velocity.  Of course, in
 * real-world applications we don't have this data;  we only have observedState.
 * @param observedState The observed or measured position and velocity.
 */
class KalmanFilterData(val label:String, 
                       val actualState:DenseVector[Double],
                       val observedState:DenseVector[Double]) 
  extends Serializable {
  override def toString():String = {
    //label+f", actualState=$actualState%.4f, observedState=$observedState%.4f"
    label+" data"+s", actualState=$actualState, observedState=$observedState"
  }
}


object KalmanFilterDataGenerator {
  def pushToRdd(ssc: StreamingContext, 
                rddQueue:SynchronizedQueue[RDD[KalmanFilterData]], pause:Int): Unit = {

    val F:DenseMatrix[Double] = KalmanFilterConstants.F
    val GA:DenseVector[Double] = KalmanFilterConstants.GA
    val H:DenseMatrix[Double] = KalmanFilterConstants.H
    val vN:Double = KalmanFilterConstants.vN
    var x:DenseVector[Double] = DenseVector(30.0, -9.0)
    var z:DenseVector[Double] = DenseVector(10.0, 0.0)  // Measured data.

    val numBatches = 66
    for (i <- 1 to numBatches) {

      x = F*x + GA     //  Model for x: x1 = F*x0 + Ga.
      // We generate the observed state z synthetically from the actual state.
      z = H*x + vN     // The observed state, including noise.

      val kfData = new KalmanFilterData("KF", x, z)
      val dataList:List[KalmanFilterData] = List(kfData)

      // "Distribute a local Scala collection to form an RDD."
      rddQueue += ssc.sparkContext.makeRDD(dataList)

      Thread.sleep(pause)
    }
  }
}
