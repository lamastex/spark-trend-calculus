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

class TrendCalculus2(timeseries: Dataset[TimePoint], windowSize: Int, spark: SparkSession, initZero: Boolean = true) {

  import spark.implicits._

  val windowSpec = Window.rowsBetween(Window.unboundedPreceding, 0)

  def getReversals: Dataset[(TimePoint, String)] = {

    val tmp = if (initZero) {
      timeseries
        .map(r => (Point(r.x.getTime(), r.y), "dummy"))
        .toDF("point","dummy")
        .withColumn("fhls", new TsToTrend(windowSize)($"point").over(windowSpec))
    } else {
      val initPoint = try {
        timeseries.first
      } catch {
        case e: NoSuchElementException => return spark.emptyDataset[(TimePoint, String)].toDF("reversalPoint", "reversal").as[(TimePoint, String)]
      }
      
      val init = Some(Row(Point(initPoint.x.getTime, initPoint.y)))

      timeseries
        .rdd.mapPartitionsWithIndex{ (id_x, iter) => if (id_x == 0) iter.drop(1) else iter }.toDS
        .map(r => (Point(r.x.getTime(), r.y), "dummy"))
        .toDF("point","dummy")
        .withColumn("fhls", new TsToTrend(windowSize, init)($"point").over(windowSpec))
    }

    tmp
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
}
