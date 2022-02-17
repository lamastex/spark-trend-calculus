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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import java.sql.Timestamp
import org.sparkproject.jetty.util.DateCache.Tick
import avro.shaded.com.google.common.base.Ticker

class TrendCalculus2(timeseries: Dataset[TickerPoint], windowSize: Int, spark: SparkSession, initZero: Boolean = true) extends Serializable {

  import spark.implicits._

  private val emptyPoint = TickerPoint("", new Timestamp(0L), 0.0)
  private val emptyFHLS = TrendCalculus2.FHLS(emptyPoint,emptyPoint,emptyPoint,emptyPoint)

  private def makeFHLS(points: Seq[TickerPoint]): TrendCalculus2.FHLS = {
    val sortedByVal = points.groupBy(_.y).map{case (v,vSeq) => (v,vSeq.sortBy(_.x.getTime))}.toSeq.sortBy(_._1)
    val high = sortedByVal.last._2.head //Should be last head
    val low = sortedByVal.head._2.last //Should be head last
    val List(first,second) = if (low.x.getTime < high.x.getTime) List(low,high) else List(high,low)
    TrendCalculus2.FHLS(first,high,low,second)
  }
  
  // Go through computed fhls to compute trends and intermediate windows for zero trends.
  private def getIntrAndTrends(prevFHLSWithTrends: Seq[TrendCalculus2.FHLSWithTrend], currFHLS: TrendCalculus2.FHLS): Seq[TrendCalculus2.FHLSWithTrend] = {
    val prevFHLSWithTrend = prevFHLSWithTrends.last
    val prevFHLS = prevFHLSWithTrend.fhls
    val prevHigh = prevFHLS.high
    val currHigh = currFHLS.high
    val prevLow = prevFHLS.low
    val currLow = currFHLS.low
    
    var currTrend = (
      (currHigh.y - prevHigh.y).signum + 
      (currLow.y - prevLow.y).signum).signum
    
    // If the trend is non-zero, no intermediate window is necessary
    if (currTrend != 0) return Seq(TrendCalculus2.FHLSWithTrend(currFHLS, currTrend))
    
    val intrFirst = prevFHLS.second
    val intrSecond = currFHLS.first
    val List(intrLow, intrHigh) = Seq(intrFirst, intrSecond).sortBy(_.y)
    
    var intrTrend = (
      (intrHigh.y - prevHigh.y).signum + 
      (intrLow.y - prevLow.y).signum).signum
    
    currTrend = (
      (currHigh.y - intrHigh.y).signum + 
      (currLow.y - intrLow.y).signum).signum
    
    if (intrTrend == 0) intrTrend = prevFHLSWithTrend.trend
    if (currTrend == 0) currTrend = intrTrend
    
    val intrFHLS = TrendCalculus2.FHLS(first=intrFirst, high=intrHigh, low=intrLow, second=intrSecond)
    
    return Seq(TrendCalculus2.FHLSWithTrend(intrFHLS, intrTrend), TrendCalculus2.FHLSWithTrend(currFHLS, currTrend))
  }

  private def trendToRev(prevFhlsWithTrend: TrendCalculus2.FHLSWithTrend, currFhlsWithTrend: TrendCalculus2.FHLSWithTrend): Option[Reversal] = {
    val rev = (currFhlsWithTrend.trend - prevFhlsWithTrend.trend).signum
    rev match {
      case 0 => None
      case 1 => Some(Reversal(prevFhlsWithTrend.fhls.low, rev))
      case _ => Some(Reversal(prevFhlsWithTrend.fhls.high, rev))
    }
  }

  def processBuffer(oldState: TrendCalculus2.State, values: Seq[TickerPoint]): (TrendCalculus2.State, Seq[Reversal]) = {
    
    val buffer: Seq[TickerPoint] = (oldState.buffer ++ values).sortBy(_.x.getTime) // merging buffered points and new input points and sorting to get right order
    val toFHLS = buffer.dropRight(buffer.length % windowSize) // need multiple of windowsize to make fhls of size windowsize
    val remainingBuffer = buffer.takeRight(buffer.length % windowSize) // remaining part of buffer, is sent to next state
    
    val windows = toFHLS.sliding(windowSize, windowSize).toList.map(_.toSeq)
    val fhlsSeq = windows.map(makeFHLS)
    
    val lastFHLSWithTrend = TrendCalculus2.FHLSWithTrend(oldState.lastFHLS, oldState.lastTrend)
    
    val fhlsWithTrendSeq = fhlsSeq.scanLeft(Seq(lastFHLSWithTrend))(getIntrAndTrends).flatten.toSeq
    
    val reversalSeq = fhlsWithTrendSeq.sliding(2,1).flatMap(ls => trendToRev(ls.head, ls.last)).toSeq
    
    val newLastFHLSWithTrend = fhlsWithTrendSeq.last
    val newState = TrendCalculus2.State(newLastFHLSWithTrend.fhls, newLastFHLSWithTrend.trend, remainingBuffer)
    
    (newState, reversalSeq)
  }

  private def reversalMap(key: String, inputs: Iterator[TickerPoint], state: GroupState[Seq[TrendCalculus2.State]]): Iterator[Reversal] = {
    
    val values: Seq[TickerPoint] = inputs.toSeq.sortBy(_.x.getTime)

    var initialState = TrendCalculus2.State(
        lastFHLS = if (initZero) emptyFHLS else makeFHLS(Seq(values.head)),
        lastTrend = 0,
        buffer = Seq[TickerPoint]()
      )

    val oldState: Seq[TrendCalculus2.State] = state.getOption.getOrElse(Seq(initialState))

    var reversalStates = Seq[TrendCalculus2.State]()
    var reversalSeq = Seq[Reversal]()
    var allReversals = Seq[Reversal]()
    var resPair = processBuffer(oldState.head, if (state.getOption.isEmpty && !initZero) values.tail else values)
    reversalStates = reversalStates :+ resPair._1
    reversalSeq = resPair._2.sortBy(_.tickerPoint.x.getTime)
    allReversals = allReversals ++ reversalSeq

    var i = 1
    // add new reversal order state if there are any reversals of one order lower
    while ( reversalSeq.nonEmpty ) {

      resPair = if (oldState.length > i)
        processBuffer(oldState(i), reversalSeq.map(_.tickerPoint))
      else {
        // initialize new reversal order
        initialState = initialState.copy(
          lastFHLS = if (initZero) emptyFHLS else makeFHLS(Seq(reversalSeq.map(_.tickerPoint).head))
        )

        processBuffer(initialState, if (!initZero) reversalSeq.map(_.tickerPoint).tail else reversalSeq.map(_.tickerPoint))
      }

      reversalStates = reversalStates :+ resPair._1
      reversalSeq = resPair._2.map(rev => rev.copy(reversal = rev.reversal * (i + 1))).sortBy(_.tickerPoint.x.getTime)
      allReversals = allReversals ++ reversalSeq
      i += 1
    }

    val highestOrderReversals = (values.map(Reversal(_, 0)) ++ allReversals)
      .groupBy(_.tickerPoint)
      .values
      .map(revSeq => revSeq.maxBy(revPoint => math.abs(revPoint.reversal)))
      .toSeq
      .sortBy(_.tickerPoint.x.getTime)

    state.update(reversalStates)
    highestOrderReversals.toIterator
 }

  private def getReversals(ts: Dataset[TickerPoint]): Dataset[Reversal] = {
    ts
      .groupByKey{ tp => tp.ticker }
      .flatMapGroupsWithState[Seq[TrendCalculus2.State], Reversal](
        outputMode = OutputMode.Append,
        timeoutConf = GroupStateTimeout.NoTimeout)(reversalMap)
      .filter($"tickerPoint.ticker" =!= "")
      .withColumn("revSign", signum($"reversal"))
      .groupBy($"tickerPoint", $"revSign")
      .agg((max(abs($"reversal")) * $"revSign").cast("int").as("reversal"))
      .drop($"revSign")
      .as[Reversal]
  }

  private def getStreamReversals(ts: Dataset[TickerPoint]): Dataset[Reversal] = {
    ts
      .groupByKey{ tp => tp.ticker }
      .flatMapGroupsWithState[Seq[TrendCalculus2.State], Reversal](
        outputMode = OutputMode.Append,
        timeoutConf = GroupStateTimeout.NoTimeout)(reversalMap)
      .filter($"tickerPoint.ticker" =!= "")
      .as[Reversal]

  }

  // If there is a streaming dataset, the final aggregation is omitted.
  def reversals: Dataset[Reversal] = if (timeseries.isStreaming) getStreamReversals(timeseries) else getReversals(timeseries)

}

object TrendCalculus2 {
  case class FHLS(first: TickerPoint, high: TickerPoint, low: TickerPoint, second: TickerPoint)  
  case class FHLSWithTrend(fhls: FHLS,trend: Int)
  case class State(lastFHLS: FHLS, lastTrend: Int, buffer: Seq[TickerPoint])
}