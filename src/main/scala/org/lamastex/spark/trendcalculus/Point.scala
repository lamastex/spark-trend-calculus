package org.lamastex.spark.trendcalculus

case class Point(
                  x: Long,
                  y: Double
                )

case class TimePoint(
  x: java.sql.Timestamp,
  y: Double
)
