package sparkstr

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.junit.ShouldMatchersForJUnit


/**
 * See http://easycalculation.com/statistics/standard-deviation.php
 */
@RunWith(classOf[JUnitRunner])
class StreamVarianceTest extends FunSuite {

  test("combinePrevCurrVars zero Vars") {
    val prevVar = 0.0
    val prevMean = 0.0
    val prevCount = 0
    val currVar = 0.0
    val currMean = 0.0
    val currCount = 1     // prevCount+currCount may not be zero.
    val expectedVar = 0.0
    val result:Double = 
      StreamVarianceUpdateFunction.combinePrevCurrVars(prevVar, prevMean, prevCount,
                                                       currVar, currMean, currCount)
    assert(expectedVar==result)
  }

  test("combinePrevCurrVars with known variances:") {
    val values = (-5 to 15).map(_.toDouble)
    val prevVar = 
      StreamVarianceUpdateFunction.sumSquaresVariance(values = values.take(3))
    assert(0.6666666666666679===prevVar)
    val prevMean = values.take(3).sum/3
    assert(-4===prevMean)
    val prevCount = 3
    val currVar = 
      StreamVarianceUpdateFunction.sumSquaresVariance(values = values.drop(3))
    assert(26.91666666666667===currVar)
    val currMean = values.drop(3).sum/(21-3)
    assert(6.5===currMean)
    val currCount = 21-3
    val expectedVar = 
      StreamVarianceUpdateFunction.sumSquaresVariance(values = values)  // = 36.6667
    val result:Double = 
      StreamVarianceUpdateFunction.combinePrevCurrVars(prevVar, prevMean, prevCount,
                                                       currVar, currMean, currCount)
    assert(Math.abs(expectedVar - result) < 0.000000001)
  }

  test("combinePrevCurrVars in loop") {
    val values = (-5 to 15).map(_.toDouble)
    val expectedVar = StreamVarianceUpdateFunction.sumSquaresVariance(values = values)
    for(i <- 1 to 20) {
      val prevVar = 
        StreamVarianceUpdateFunction.sumSquaresVariance(values = values.take(i))
      val prevMean = values.take(i).sum/i
      val prevCount = i
      val currVar = 
        StreamVarianceUpdateFunction.sumSquaresVariance(values = values.drop(i))
      val currMean = values.drop(i).sum/(21-i)
      val currCount = 21-i
      val result:Double = 
        StreamVarianceUpdateFunction.combinePrevCurrVars(prevVar, prevMean, prevCount,
                                                         currVar, currMean, currCount)
      assert(Math.abs(expectedVar - result) < 0.000000001)
    }
  }


  test("sumSquaresVar() with known variance") {
    val values:Seq[Double] = List[Double](-5.0, -4.0, -3.0)
    val result = StreamVarianceUpdateFunction.sumSquaresVariance(values)
    assert(0.6666666666666679===result)
  }

  test("sumSquaresVar() with zeros") {
    val values:Seq[Double] = List[Double](0.0, 0.0, 0.0)
    val result = StreamVarianceUpdateFunction.sumSquaresVariance(values)
    assert(0.0===result, s"Expected zero, got $result")
  }

  test("sumSquaresVar() prevVar=0, mean=0.") {
    val values:Seq[Double] = List[Double](-2, 2, -4, 4)
    val result = StreamVarianceUpdateFunction.sumSquaresVariance(values)
    val expected: Double = (-2* -2 + 2*2 + -4* -4 + 4*4) / 4    // = 10.0
    assert(expected===result)
  }

  test("sumSquaresVar() prevVar=0, mean=1.") {
    val values:Seq[Double] = List[Double](-1, 3, -2, 4)
    val result = StreamVarianceUpdateFunction.sumSquaresVariance(values)
    val expected: Double = ((-1.0* -1 + 3*3 + -2* -2 + 4*4) / 4) - 1*1   // = 6.5
    assert(expected===result)
  }

  test("sumSquaresVar() prevVar=0, mean=2.") {
    val values:Seq[Double] = List[Double](0.0, 4, -1, 5)
    val result = StreamVarianceUpdateFunction.sumSquaresVariance(values)
    val m: Double = values.sum/values.length    // = 2
    val expected: Double = 
      ((-0.0* -0.0 + 4*4 + -1* -1 + 5*5) / 4) - m * m // = 10.5-4=6.5
    assert(expected===result)
  }


  test("welfordVariance(List(-1, 3, -2, 4), 0.0, 0.0, 0)") {
    val values: Seq[Double] = List(-1, 3, -2, 4)
    val result = StreamVarianceUpdateFunction.welfordVariance(values)
    val expected: Double = ((-1.0* -1 + 3*3 + -2* -2 + 4*4) / 4) - 1*1   // = 6.5
    assert(expected===result)
  }

  test("welfordVariance vs. sumSquaresVariance") {
    val start = 1
    val end = 2
    for(i <- start to end) {
      val values: Seq[Double] = (1 to i).toList.map(_.toDouble)
      val result = StreamVarianceUpdateFunction.welfordVariance(values)
      val expected = StreamVarianceUpdateFunction.sumSquaresVariance(values)
      assert(expected===result)
    }
  }

}
