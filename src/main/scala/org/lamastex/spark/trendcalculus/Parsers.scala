package org.lamastex.spark.trendcalculus

import java.sql.Timestamp
import java.text.SimpleDateFormat

object Parsers {

  def parseFX(str: String): FXData = {
    val tokens = str.split(";")

    val time: Option[Timestamp] = try {
      Some(new Timestamp(new SimpleDateFormat("yyyyMMdd HHmmSS").parse(tokens(0)).getTime))
    } finally {
      None
    }

    FXData(
      time = time,
      open = try {Some(tokens(1).toDouble)} finally { None },
      high = try {Some(tokens(2).toDouble)} finally { None },
      low = try {Some(tokens(3).toDouble)} finally { None },
      close = try {Some(tokens(4).toDouble)} finally { None },
      volume = try {Some(tokens(5).toInt)} finally { None }
    )
  }

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
      case e: NumberFormatException => try {
        Some(tokens(7).toDouble.toInt)
      } catch {
        case _: Throwable => None
      }
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