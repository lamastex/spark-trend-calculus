package org.lamastex.spark.trendcalculus

object SeriesUtils {

  def movingAverage(timeseries: Array[Point], grouping: Frequency.Value): Unit = {
    timeseries.groupBy(p => DateUtils.roundTime(grouping, p.x)).flatMap[Point]({ case (group, groupedSeries) =>
      val avg = groupedSeries.map(_.y).sum / groupedSeries.length
      groupedSeries.map(p => {
        Point(p.x, avg)
      })
    }).toArray.sortBy(_.x)
  }

  def completeSeries(timeseries: Array[Point], frequency: Frequency.Value, fillingStrategy: FillingStrategy.Value): Array[Point] = {
    val completeWithMissingValues = DateUtils.indices(frequency, timeseries.head.x, timeseries.last.x).map(l => {
      (l, None: Option[Double])
    }).union(timeseries.map(point => {
      (point.x, Some(point.y))
    })).groupBy({ case (x, y) =>
      DateUtils.roundTime(frequency, x)
    }).map({ case (group, xys) =>
      val defined = xys.filter(_._2.isDefined).map(_._2.get)
      if (defined.isEmpty) {
        (group, None: Option[Double])
      } else {
        (group, Some(defined.sum / defined.size))
      }
    }).toArray.sortBy(_._1)

    if (!completeWithMissingValues.exists(_._2.isEmpty)) {
      completeWithMissingValues.map { case (x, optY) =>
        Point(x, optY.get)
      }
    } else {
      fillMissingData(completeWithMissingValues, fillingStrategy)
    }
  }

  private def fillMissingData(data: Array[(Long, Option[Double])], fillingStrategy: FillingStrategy.Value): Array[Point] = {
    var i = 0
    val points = collection.mutable.ArrayBuffer[Point]()
    while (i < data.length) {
      if (data(i)._2.isDefined) {
        points += Point(data(i)._1, data(i)._2.get)
        i += 1
      } else {
        var needCover = true
        val toCover = collection.mutable.ArrayBuffer[Long]()
        toCover += data(i)._1
        var j = i + 1
        while (j < data.length && needCover) {
          if (data(j)._2.isEmpty) {
            toCover += data(j)._1
            j += 1
          } else {
            needCover = false
            val p1 = points.last.y
            val p2 = data(j)._2.get
            points ++= fillBlanks(p1, p2, toCover.toArray, fillingStrategy)
          }
        }
        i = j
      }
    }
    points.toArray
  }

  private def fillBlanks(p1: Double, p2: Double, data: Array[Long], fillingStrategy: FillingStrategy.Value) = {
    val toCover = data.zipWithIndex
    val toCoverOffset = toCover.map(_._2).max
    fillingStrategy match {
      case FillingStrategy.ZERO =>
        toCover.map({ case (x, _) =>
          Point(x, 0.0d)
        })
      case FillingStrategy.LINEAR =>
        val a = (p2 - p1) / (toCoverOffset + 2)
        toCover.map({ case (x, offset) =>
          Point(x, a * (offset + 1) + p1)
        })
      case FillingStrategy.MEAN =>
        val mean = (p2 + p1) / 2.0
        toCover.map({ case (x, _) =>
          Point(x, mean)
        })
      case FillingStrategy.LOCF =>
        toCover.map({ case (x, _) =>
          Point(x, p1)
        })
    }
  }

  def normalizeSeries(data: Array[Point]): Array[Point] = {
    val avg = data.map(p => p.y).sum / data.length
    val std = math.sqrt(data.map(p => math.pow(p.y - avg, 2)).sum / (data.length - 1))
    data.map { point =>
      Point(point.x, (point.y - avg) / std)
    }
  }

  def normalizeSeries(data: Array[Point], avg: Double, std: Double): Array[Point] = {
    data.map { point =>
      Point(point.x, (point.y - avg) / std)
    }
  }

  def aggregateSeries(timeseries: Array[Point], frequency: Frequency.Value, aggregateStrategy: AggregateStrategy.Value): Array[Point] = {
    val actualFreq = DateUtils.findFrequency(timeseries.map(_.x))
    require(DateUtils.getMilliseconds(actualFreq) < DateUtils.getMilliseconds(frequency))
    timeseries
      .groupBy { point =>
        DateUtils.roundTime(frequency, point.x)
      }
      .map { case (x, it) =>
        val ys = it.map(_.y)
        val sum = ys.sum
        aggregateStrategy match {
          case AggregateStrategy.MEAN =>
            Point(x, sum / ys.length)
          case AggregateStrategy.SUM =>
            Point(x, sum)
        }
      }
      .toArray
      .sorted(Ordering.by((p: Point) => p.x))
  }

}
