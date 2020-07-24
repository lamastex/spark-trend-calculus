package org.lamastex.trendcalculus

import java.sql.Timestamp
import java.text.SimpleDateFormat

object YFinance {

  case class YFData(
    time: Option[Timestamp] = None,
    ticker: Option[String] = None,
    open: Option[Double] = None,
    high: Option[Double] = None,
    low: Option[Double] = None,
    close: Option[Double] = None,
    adjClose: Option[Double] = None,
    volume: Option[Int] = None
  )

  def parseYF(str: String): YFData = {
    val tokens = str.split(",")

    val time: Option[Timestamp] = try {
      val rawTime = tokens(0)
      if (rawTime.length <= 10) {
        Some(new Timestamp(new SimpleDateFormat("yyyy-MM-dd").parse(rawTime).getTime))
      } else {
        Some(new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").parse(rawTime.take(19)).getTime))
      }
    } catch {
      case _: Throwable => None
    }

    val volume: Option[Int] = try {
      Some(tokens(7).toInt)
    } catch {
      case e: NumberFormatException => Some(tokens(7).toDouble.toInt)
      case _: Throwable => None
    }

    YFData(
        time = time,
        ticker = try {Some(tokens(1))} catch {case _: Throwable => None},
        open = try {Some(tokens(2).toDouble)} catch {case _: Throwable => None},
        high = try {Some(tokens(3).toDouble)} catch {case _: Throwable => None},
        low = try {Some(tokens(4).toDouble)} catch {case _: Throwable => None},
        close = try {Some(tokens(5).toDouble)} catch {case _: Throwable => None},
        adjClose = try {Some(tokens(6).toDouble)} catch {case _: Throwable => None},
        volume = volume
    )
  }
}