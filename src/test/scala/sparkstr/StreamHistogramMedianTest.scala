package sparkstr

import org.junit.runner.RunWith

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalatest._
import org.scalatest.prop.Checkers
import org.scalatest.exceptions.TestFailedException

import org.scalatest.junit.JUnitRunner
import org.scalatest.junit.ShouldMatchersForJUnit


/**
 * Test StreamHistogramMedian.
 */
@RunWith(classOf[JUnitRunner])
class StreamHistogramMedianTest extends FunSuite  with Checkers {

  test("findHistogram and median") {
    val binEndPts = Array(0.0, 1.0, 2.0, 3.0, 4.0)
    var values: Array[Double] = Array()
    var resultH = HistogramMedianUpdateFunction.findHistogram(values, binEndPts)
    var expectedH: Array[Long] = Array(0, 0, 0, 0)
    assert(expectedH === resultH)
    // Median of an empty histogram doesn't make sense and we don't handle it.

    values = Array(0.1, 1.1)
    resultH = HistogramMedianUpdateFunction.findHistogram(values, binEndPts)
    expectedH = Array(1, 1, 0, 0)
    assert(expectedH === resultH)
    var medianIndex = values.size / 2
    var resultM = HistogramMedianUpdateFunction.findHistogramMedian(
      resultH, binEndPts, medianIndex)
    var expectedM: Double = 1.5
    assert(expectedM === resultM)

    values = Array(0.1, 1.1, 2.1, 3.1)
    resultH = HistogramMedianUpdateFunction.findHistogram(values, binEndPts)
    expectedH = Array(1, 1, 1, 1)
    assert(expectedH === resultH)
    medianIndex = values.size / 2
    resultM = HistogramMedianUpdateFunction.findHistogramMedian(
      resultH, binEndPts, medianIndex)
    expectedM = 2.5
    assert(expectedM === resultM)

    values = Array(0, 1, 2, 3)
    resultH = HistogramMedianUpdateFunction.findHistogram(values, binEndPts)
    expectedH = Array(1, 1, 1, 1)
    assert(expectedH === resultH)
    medianIndex = values.size / 2
    resultM = HistogramMedianUpdateFunction.findHistogramMedian(
      resultH, binEndPts, medianIndex)
    expectedM = 2.5
    assert(expectedM === resultM)

    values = Array(0, 0.5, 1, 1.1, 2, 2.3, 2.8, 3, 3, 3.3, 3.6)
    resultH = HistogramMedianUpdateFunction.findHistogram(values, binEndPts)
    expectedH = Array(2, 2, 3, 4)
    assert(expectedH === resultH)
    medianIndex = values.size / 2
    resultM = HistogramMedianUpdateFunction.findHistogramMedian(
      resultH, binEndPts, medianIndex)
    expectedM = 2.5
    assert(expectedM === resultM)

    values = Array(0, 0.5, 1, 1.1, 2, 2.3, 2.8, 3.1, 3.1, 3.2, 3.4, 3.5, 3.6, 3.8, 3.9)
    resultH = HistogramMedianUpdateFunction.findHistogram(values, binEndPts)
    expectedH = Array(2, 2, 3, 8)
    assert(expectedH === resultH)
    medianIndex = values.size / 2
    resultM = HistogramMedianUpdateFunction.findHistogramMedian(
      resultH, binEndPts, medianIndex)
    val actualM = 3.1
    assert(Math.abs(actualM - resultM) < 0.5000001)
  }




}

