package org.lamastex.spark.trendcalculus

case class Point(
                  x: Long,
                  y: Double
                )

case class TickerPoint(
  ticker: String,
  x: java.sql.Timestamp,
  y: Double
)

case class Reversal(
  tickerPoint: TickerPoint,
  reversal: Int
)

case class FlatReversal(
  ticker: String,
  x: java.sql.Timestamp,
  y: Double,
  reversal: Int
)
