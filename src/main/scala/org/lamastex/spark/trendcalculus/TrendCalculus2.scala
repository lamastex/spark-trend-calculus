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

class TrendCalculus2(timeseries: Dataset[TickerPoint], windowSize: Int, spark: SparkSession, initZero: Boolean = true) extends Serializable {

  import spark.implicits._

  private case class FHLS(first: TickerPoint, high: TickerPoint, low: TickerPoint, second: TickerPoint)  
  private case class FHLSWithTrend(fhls: FHLS,trend: Int)
  private case class State(lastFHLS: FHLS, lastTrend: Int, buffer: Seq[TickerPoint])

  private val emptyPoint = TickerPoint("", new Timestamp(0L), 0.0)
  private val emptyFHLS = FHLS(emptyPoint,emptyPoint,emptyPoint,emptyPoint)

  private def makeFHLS(points: Seq[TickerPoint]): FHLS = {
    val sortedByVal = points.groupBy(_.y).map{case (v,vSeq) => (v,vSeq.sortBy(_.x.getTime))}.toSeq.sortBy(_._1)
    val high = sortedByVal.last._2.head //Should be last head
    val low = sortedByVal.head._2.last //Should be head last
    val List(first,second) = if (low.x.getTime < high.x.getTime) List(low,high) else List(high,low)
    FHLS(first,high,low,second)
  }
  
  // Go through computed fhls to compute trends and intermediate windows for zero trends.
  private def getIntrAndTrends(prevFHLSWithTrends: Seq[FHLSWithTrend], currFHLS: FHLS): Seq[FHLSWithTrend] = {
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
    if (currTrend != 0) return Seq(FHLSWithTrend(currFHLS, currTrend))
    
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
    
    val intrFHLS = FHLS(first=intrFirst, high=intrHigh, low=intrLow, second=intrSecond)
    
    return Seq(FHLSWithTrend(intrFHLS, intrTrend), FHLSWithTrend(currFHLS, currTrend))
  }

  private def trendToRev(prevFhlsWithTrend: FHLSWithTrend, currFhlsWithTrend: FHLSWithTrend): Option[Reversal] = {
    val rev = (currFhlsWithTrend.trend - prevFhlsWithTrend.trend).signum
    rev match {
      case 0 => None
      case 1 => Some(Reversal(prevFhlsWithTrend.fhls.low, rev))
      case _ => Some(Reversal(prevFhlsWithTrend.fhls.high, rev))
    }
  }

  private def reversalMap(key: String, inputs: Iterator[TickerPoint], state: GroupState[State]): Iterator[Reversal] = {
    
    val values: Seq[TickerPoint] = inputs.toSeq

    val initialState = State(
        lastFHLS = if (initZero) emptyFHLS else makeFHLS(Seq(values.sortBy(_.x.getTime).head)),
        lastTrend = 0,
        buffer = Seq[TickerPoint]()
      )

    val oldState: State = state.getOption.getOrElse(initialState)
    
    val buffer: Seq[TickerPoint] = (oldState.buffer ++ (if (state.getOption.isEmpty && !initZero) values.tail else values)).sortBy(_.x.getTime) // merging buffered points and new input points and sorting to get right order
    val toFHLS = buffer.dropRight(buffer.length % windowSize) // need multiple of windowsize to make fhls of size windowsize
    val remainingBuffer = buffer.takeRight(buffer.length % windowSize) // remaining part of buffer, is sent to next state
    
    val windows = toFHLS.sliding(windowSize, windowSize).toList.map(_.toSeq)
    val fhlsSeq = windows.map(makeFHLS)
    
    val lastFHLSWithTrend = FHLSWithTrend(oldState.lastFHLS, oldState.lastTrend)
    
    val fhlsWithTrendSeq = fhlsSeq.scanLeft(Seq(lastFHLSWithTrend))(getIntrAndTrends).flatten.toSeq
    
    val reversalIter = fhlsWithTrendSeq.sliding(2,1).flatMap(ls => trendToRev(ls.head, ls.last))
    
    val newLastFHLSWithTrend = fhlsWithTrendSeq.last
    val newState = State(newLastFHLSWithTrend.fhls, newLastFHLSWithTrend.trend, remainingBuffer)
    state.update(newState)
    
    reversalIter
  }

  private def getReversals(ts: Dataset[TickerPoint]): Dataset[Reversal] = {
    ts
      .groupByKey{ tp => tp.ticker }
      .flatMapGroupsWithState[State, Reversal](
        outputMode = OutputMode.Append,
        timeoutConf = GroupStateTimeout.NoTimeout)(reversalMap)
      .filter($"tickerPoint.ticker" =!= "")
      
  }

  def reversals: Dataset[Reversal] = getReversals(timeseries)

  def nReversals(numReversals: Int): Seq[Dataset[Reversal]] = {
    var tmpDSs: Seq[Dataset[Reversal]] = Seq(reversals)
    for (i <- (2 to numReversals)) {
      tmpDSs = tmpDSs :+ getReversals(tmpDSs.last.toDF.select($"tickerPoint.ticker", $"tickerPoint.x", $"tickerPoint.y").as[TickerPoint])
    }

    tmpDSs
  }

  def nReversalsJoined(numReversals: Int): DataFrame = {
    val revDSs = nReversals(numReversals)
    revDSs.map(_.cache.count)
    val joinedDF = revDSs
      .zipWithIndex
      .map{ case (ds: Dataset[Reversal], i: Int) => ds.toDF.withColumnRenamed("reversal", s"reversal${i+1}") }
      .foldLeft(timeseries.toDF)( (acc: DataFrame, ds: DataFrame) => acc.join(ds, $"x" === $"tickerPoint.x", "left").drop("tickerPoint") )
    joinedDF
  }

  def nReversalsJoinedWithMaxRev(numReversals: Int): DataFrame = {
    val joinedDF = nReversalsJoined(numReversals)

    val dfWithMaxRev = joinedDF.map{ r =>
      val maxRev: Int = (3 to numReversals+2).find(r.isNullAt(_)).getOrElse(numReversals+3) - 3
      (r.getAs[Timestamp](1),maxRev)
    }.toDF("x","maxRev")

    val joinedDFWithMaxRev = joinedDF.join(dfWithMaxRev, "x").orderBy("x")
    joinedDFWithMaxRev
  }
}
