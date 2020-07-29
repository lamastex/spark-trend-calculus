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

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import java.sql.Timestamp

class TrendCalculus2(timeseries: Dataset[TimePoint], windowSize: Int, spark: SparkSession) {

  import spark.implicits._

  val windowSpec = Window.rowsBetween(Window.unboundedPreceding, 0)

  def getReversals: Dataset[(TimePoint, String)] = {
    timeseries
      .map(r => (Point(r.x.getTime(), r.y), "dummy"))
      .toDF("point","dummy")
      .withColumn("fhls", new TsToTrend(windowSize)($"point").over(windowSpec))
      .select(explode($"fhls") as "tmp")
      .select($"tmp.fhls".as("fhls"), $"tmp.trend".as("trend"), $"tmp.lastTrend".as("lastTrend"), $"tmp.lastFhls".as("lastFHLS"), $"tmp.reversal".as("reversal"))
      .filter($"reversal" =!= 0)
      .select($"lastFHLS", $"reversal" as "reversalInt")
      .withColumn("reversalPoint", when($"reversalInt" === -1, $"lastFHLS.high").otherwise($"lastFHLS.low"))
      .withColumn("reversal", when($"reversalInt" === -1, lit("Top")).otherwise(lit("Bottom")))
      .select($"reversalPoint", $"reversal")
      .filter($"reversalPoint.x" =!= 0L)
      .map( r => (TimePoint(new Timestamp(r.getStruct(0).getLong(0)), r.getStruct(0).getDouble(1)), r.getString(1)))
      .select($"_1" as "reversalPoint", $"_2" as "reversal")
      .as[(TimePoint,String)]
  }

  private def tsToFHLSWithRev(ts: Dataset[Point]): Dataset[Row] = {
    ts
      .map(r => (Point(r.x, r.y), "dummy"))
      .toDF("point","dummy")
      .withColumn("fhls", new TsToTrend(windowSize)($"point").over(windowSpec))
      .select(explode($"fhls") as "tmp")
      .select($"tmp.fhls".as("fhls"), $"tmp.trend".as("trend"), $"tmp.lastTrend".as("lastTrend"), $"tmp.lastFhls".as("lastFHLS"), $"tmp.reversal".as("reversal"))
  }

  def trendToRev(df: Dataset[Row]): Dataset[(Point, Int)] = {
    df
      .filter($"reversal" =!= 0)
      .select($"lastFHLS", $"reversal")
      .withColumn("reversalPoint", when($"reversal" === -1, $"lastFHLS.high").otherwise($"lastFHLS.low"))
      .select($"reversalPoint", $"reversal")
      .as[(Point,Int)]
      .filter($"reversalPoint.x" =!= 0L)
  }
}
