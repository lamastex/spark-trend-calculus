package com.aamend.texata.timeseries

import org.joda.time.{DateTime, DateTimeConstants}

import scala.util.Try

object DateUtils {

  val frequencyMillisecond: List[FrequencyMillisecond] = {
    List(
      FrequencyMillisecond(Frequency.MILLI_SECOND, 1L),
      FrequencyMillisecond(Frequency.SECOND, 1000L),
      FrequencyMillisecond(Frequency.MINUTE, 1000L * 60),
      FrequencyMillisecond(Frequency.HOUR, 1000L * 60 * 60),
      FrequencyMillisecond(Frequency.DAY, 1000L * 60 * 60 * 24),
      FrequencyMillisecond(Frequency.WEEK, 1000L * 60 * 60 * 24 * 7),
      FrequencyMillisecond(Frequency.MONTH, 1000L * 60 * 60 * 24 * 30),
      FrequencyMillisecond(Frequency.QUARTER, 1000L * 60 * 60 * 24 * 30 * 3),
      FrequencyMillisecond(Frequency.HALF_YEAR, 1000L * 60 * 60 * 24 * (365 / 2)),
      FrequencyMillisecond(Frequency.YEAR, 1000L * 60 * 60 * 24 * 365)
    )
      .sortBy(_.milliseconds)
      .reverse
  }
  val monthYearMap = Map(
    DateTimeConstants.JANUARY -> MonthYear(DateTimeConstants.JANUARY, DateTimeConstants.JANUARY),
    DateTimeConstants.FEBRUARY -> MonthYear(DateTimeConstants.JANUARY, DateTimeConstants.JANUARY),
    DateTimeConstants.MARCH -> MonthYear(DateTimeConstants.JANUARY, DateTimeConstants.JANUARY),
    DateTimeConstants.APRIL -> MonthYear(DateTimeConstants.APRIL, DateTimeConstants.JANUARY),
    DateTimeConstants.MAY -> MonthYear(DateTimeConstants.APRIL, DateTimeConstants.JANUARY),
    DateTimeConstants.JUNE -> MonthYear(DateTimeConstants.APRIL, DateTimeConstants.JANUARY),
    DateTimeConstants.JULY -> MonthYear(DateTimeConstants.JULY, DateTimeConstants.JULY),
    DateTimeConstants.AUGUST -> MonthYear(DateTimeConstants.JULY, DateTimeConstants.JULY),
    DateTimeConstants.SEPTEMBER -> MonthYear(DateTimeConstants.JULY, DateTimeConstants.JULY),
    DateTimeConstants.OCTOBER -> MonthYear(DateTimeConstants.OCTOBER, DateTimeConstants.JULY),
    DateTimeConstants.NOVEMBER -> MonthYear(DateTimeConstants.OCTOBER, DateTimeConstants.JULY),
    DateTimeConstants.DECEMBER -> MonthYear(DateTimeConstants.OCTOBER, DateTimeConstants.JULY)
  )

  def getMilliseconds(frequency: Frequency.Value): Long = {
    frequencyMillisecond.filter(_.frequency == frequency).head.milliseconds
  }

  def indices(fq: Frequency.Value, fromTime: Long, toTime: Long): List[Long] = {
    var from = new DateTime(DateUtils.roundTime(fq, fromTime))
    val to = new DateTime(DateUtils.roundTime(fq, toTime))
    val indices = collection.mutable.MutableList[Long]()
    while (from.isBefore(to)) {
      indices += from.toDate.getTime
      from = fq match {
        case Frequency.MILLI_SECOND => from.plusMillis(1)
        case Frequency.SECOND => from.plusSeconds(1)
        case Frequency.MINUTE => from.plusMinutes(1)
        case Frequency.HOUR => from.plusHours(1)
        case Frequency.DAY => from.plusDays(1)
        case Frequency.WEEK => from.plusWeeks(1)
        case Frequency.MONTH => from.plusMonths(1)
        case Frequency.QUARTER => from.plusMonths(3)
        case Frequency.HALF_YEAR => from.plusMonths(6)
        case Frequency.YEAR => from.plusYears(1)
      }
    }
    indices.toList
  }

  def roundTime(fq: Frequency.Value, time: Long): Long = {
    val dt = new DateTime(time)
    fq match {
      case Frequency.MILLI_SECOND => dt.toDate.getTime
      case Frequency.SECOND => dt.secondOfDay().roundFloorCopy().toDate.getTime
      case Frequency.MINUTE => dt.minuteOfDay().roundFloorCopy().toDate.getTime
      case Frequency.HOUR => dt.hourOfDay().roundFloorCopy().toDate.getTime
      case Frequency.DAY => dt.dayOfYear().roundFloorCopy().toDate.getTime
      case Frequency.WEEK => dt.weekOfWeekyear().roundFloorCopy().toDate.getTime
      case Frequency.MONTH => dt.monthOfYear().roundFloorCopy().toDate.getTime
      case Frequency.QUARTER => dt.withMonthOfYear(monthYearMap(dt.monthOfYear().get()).quarter).monthOfYear().roundFloorCopy().toDate.getTime
      case Frequency.HALF_YEAR => dt.withMonthOfYear(monthYearMap(dt.monthOfYear().get()).half).monthOfYear().roundFloorCopy().toDate.getTime
      case Frequency.YEAR => dt.year().roundFloorCopy().toDate.getTime
    }
  }

  def findFrequency(timeseries: Array[Long]): Frequency.Value = {
    Try {
      val avgDiff = timeseries.sliding(2)
        .map { array =>
          array.last - array.head
        }
        .sum / (timeseries.length - 1)
      DateUtils.frequencyMillisecond
        .dropWhile(d => d.milliseconds > avgDiff)
        .head
        .frequency
    }.getOrElse(Frequency.UNKWOWN)
  }

  def unitDifference(frequency: Frequency.Value, t1: Long, t2: Long): Double = {
    val msPerGregorianYear = 365.25 * 86400 * 1000
    frequency match {
      case Frequency.MILLI_SECOND => (t2 - t1).toDouble
      case Frequency.SECOND => (t2 - t1).toDouble / 1000
      case Frequency.MINUTE => (t2 - t1).toDouble / (1000 * 60)
      case Frequency.HOUR => (t2 - t1).toDouble / (1000 * 60 * 60)
      case Frequency.DAY => (t2 - t1).toDouble / (1000 * 60 * 60 * 24)
      case Frequency.WEEK => (t2 - t1).toDouble / (1000 * 60 * 60 * 7)
      case Frequency.MONTH => 12 * (t2 - t1).toDouble / msPerGregorianYear
      case Frequency.QUARTER => 4 * (t2 - t1).toDouble / msPerGregorianYear
      case Frequency.HALF_YEAR => 2 * (t2 - t1).toDouble / msPerGregorianYear
      case Frequency.YEAR => (t2 - t1).toDouble / msPerGregorianYear
    }
  }

  case class FrequencyMillisecond(
                                   frequency: Frequency.Value,
                                   milliseconds: Long
                                 )

  case class MonthYear(
                        quarter: Int,
                        half: Int
                      )

  object Frequency extends Enumeration with Serializable {
    val UNKWOWN, MILLI_SECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, HALF_YEAR, YEAR = Value
  }


}
