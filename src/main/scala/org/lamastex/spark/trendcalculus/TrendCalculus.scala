/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lamastex.spark.trendcalculus

    val fhlsSpec = Window.partitionBy($"window_start")

    val xMoveUDF = udf({ (x: Long, y: Double, compare: Double, defaultZero: Boolean) => 
      // Used to move x into row where y is a certain value.
      // dafaultZero controls if the smallest or largest value of x in window is to be used
      if (defaultZero) {
        if (y == compare) x else 0
      } else {
        if (y == compare) x else Long.MaxValue
      }
    })

    val yMoveUDF = udf({ (x: Long, y: Double, compare: Long, defaultZero: Boolean) => 
      // Used to move y into row where x is a certain value.
      // dafaultZero controls if the smallest or largest value of y in window is to be used
      if (defaultZero) {
        if (x == compare) y else 0
      } else {
        if (x == compare) y else Double.MaxValue
      }
    })

    val fhlsSignUDF = udf({ (first_y: Double, second_y: Double) => 
      if (first_y < second_y) 1 else -1
    })

    val dfWithHighAndLow = { 
      dfWithWindows
        .withColumn("high_y", max($"y").over(fhlsSpec))
        .withColumn("high_x", min(xMoveUDF($"x",$"y",$"high_y",lit(false))).over(fhlsSpec))
        .withColumn("low_y", min($"y").over(fhlsSpec))
        .withColumn("low_x", max(xMoveUDF($"x",$"y",$"low_y",lit(true))).over(fhlsSpec))
    }

    val resDF = {
      dfWithHighAndLow
        .withColumn("first_x", least($"low_x", $"high_x"))
        .withColumn("first_y", max(yMoveUDF($"x",$"y",$"first_x",lit(true))).over(fhlsSpec))
        .withColumn("second_x", greatest($"low_x", $"high_x"))
        .withColumn("second_y", max(yMoveUDF($"x",$"y",$"second_x",lit(true))).over(fhlsSpec))
        .withColumn("fhls_sign", fhlsSignUDF($"first_y", $"second_y"))
    }

    resDF
    //timeseries
  }

import scala.util.Try

class TrendCalculus(timeseries: Array[Point], groupingFrequency: Frequency.Value) extends Serializable {

  val map: Map[DateUtils.Frequency.Value, Long] = DateUtils.frequencyMillisecond.map(t => (t.frequency, t.milliseconds)).toMap
  val samplingFrequency: DateUtils.Frequency.Value = DateUtils.findFrequency(timeseries.map(_.x))
  val groupingMilliseconds: Long = map(groupingFrequency)
  val samplingMilliseconds: Long = map(samplingFrequency)

  require(
    groupingMilliseconds > samplingMilliseconds,
    s"Grouping frequency [$groupingFrequency] must be strictly higher than sampling frequency [$samplingFrequency]"
  )

  def getTrends: List[Trend] = {

    val fhlsSeries = timeseries
      .groupBy { point =>
        window(point.x, groupingFrequency)
      }
      .map { case (_, wSeries) =>

        val sortedWSeries = wSeries
          .groupBy { point =>
            point.y
          }
          .map { case (y, ySeries) =>
            (y, ySeries.sortBy(_.x))
          }
          .toList
          .sortBy(_._1)

        val high = sortedWSeries.last._2.head //earliest high price
        val low = sortedWSeries.head._2.last //latest low price

        val List(left, right) = List(high, low).sorted(Ordering.by((p: Point) => p.x))
        val leftSeries = wSeries.filter(_.x < left.x)
        val rightSeries = wSeries.filter(_.x > right.x)
        FHLS(
          left,
          right,
          high,
          low,
          leftSeries,
          rightSeries,
          if (left.y < right.y) 1 else -1
        )
      }
      .toList
      .sortBy(_.left.x)

    reversalTrend(fhlsSeries.tail, List(fhlsSeries.head))
      .sliding(2)
      .map { it =>
        val previousFHLS = it.head
        val currentFHLS = it.last
        val reversal = {
          (currentFHLS.sign compare previousFHLS.sign).signum match {
            case 1 => Some(previousFHLS.low)
            case -1 => Some(previousFHLS.high)
            case 0 => None: Option[Point]
          }
        }

        Trend(
          Try(currentFHLS.leftSeries.head).getOrElse(currentFHLS.left),
          Try(currentFHLS.rightSeries.last).getOrElse(currentFHLS.right),
          currentFHLS.high,
          currentFHLS.low,
          reversal,
          (currentFHLS.sign compare previousFHLS.sign).signum match {
            case 1 => TrendType.LOW
            case -1 => TrendType.HIGH
            case 0 => TrendType.NEUTRAL
          }
        )
      }
      .toList

  }

  private def getSign(current: FHLS, previous: FHLS) = {
    (
      ((current.high.y - previous.high.y) compare 0).signum +
        ((current.low.y - previous.low.y) compare 0).signum
        compare 0
      ).signum
  }

  private def getSign(currentHigh: Point, currentLow: Point, previous: FHLS) = {
    (
      ((currentHigh.y - previous.high.y) compare 0).signum +
        ((currentLow.y - previous.low.y) compare 0).signum
        compare 0
      ).signum
  }

  private def reversalTrend(windows: List[FHLS], processed: List[FHLS]): List[FHLS] = {

    def foldFunc(processedWindows: List[FHLS], windowHead: FHLS): List[FHLS] = {
      // Retrieve last processed
      val previousFHLS = processedWindows.last

      val currentFHLS = windowHead

      val sign = getSign(currentFHLS, previousFHLS)
      if (sign != 0) {
        val currentFHLSUpdated = FHLS(
          currentFHLS.left,
          currentFHLS.right,
          currentFHLS.high,
          currentFHLS.low,
          currentFHLS.leftSeries,
          currentFHLS.rightSeries,
          sign
        )

        return processedWindows :+ currentFHLSUpdated
      }

      // Shrink its rightSeries
      val previousFHLSUpdated = FHLS(
        previousFHLS.left,
        previousFHLS.right,
        previousFHLS.high,
        previousFHLS.low,
        previousFHLS.leftSeries,
        Array(),
        previousFHLS.sign
      )

      // Build an intermediate series
      val wSeries = previousFHLS.rightSeries ++ currentFHLS.leftSeries
      val left = previousFHLS.right
      val right = currentFHLS.left
      val List(low, high) = List(left, right).sorted(Ordering.by((p: Point) => p.y))
      val leftSeries = wSeries.filter(_.x < left.x)
      val rightSeries = wSeries.filter(_.x > right.x)
      val intermediateSign = getSign(high, low, previousFHLS)

      // Compute the intermediate FHLS
      val intermediateFHLS = FHLS(
        left,
        right,
        high,
        low,
        leftSeries,
        rightSeries,
        intermediateSign
      )

      // Modify sign of current list and shrink its left series
      val currentSign = getSign(currentFHLS, intermediateFHLS)
      val currentFHLSUpdated = FHLS(
        currentFHLS.left,
        currentFHLS.right,
        currentFHLS.high,
        currentFHLS.low,
        Array(),
        currentFHLS.rightSeries,
        currentSign
      )

      // Remove last window
      // Insert modified last window
      // Insert intermediate window
      // Insert modified window

        processedWindows.dropRight(1) :+
          previousFHLSUpdated :+
          intermediateFHLS :+
          currentFHLSUpdated
    }

    if (processed.isEmpty) {
      windows.tail.foldLeft(List(windows.head))(foldFunc)
    } else {
      windows.foldLeft(processed)(foldFunc)
    }
  }

  private def window(time: Long, frequency: Frequency.Value) = {
    DateUtils.roundTime(frequency, time)
  }

  private case class FHLS(
                           left: Point,
                           right: Point,
                           high: Point,
                           low: Point,
                           leftSeries: Array[Point],
                           rightSeries: Array[Point],
                           sign: Int
                         )

}

object TrendType extends Enumeration with Serializable {
  val LOW, HIGH, NEUTRAL = Value
}

case class Trend(
                  windowStart: Point,
                  windowEnd: Point,
                  high: Point,
                  low: Point,
                  reversal: Option[Point],
                  trend: TrendType.Value
                )
