package com.aamend.texata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class Brent extends App {

  val spark = SparkSession.builder().appName("gdelt-harness").getOrCreate()
  val sqlContext = spark.sqlContext

  import spark.implicits._

  val dateUDF = udf((s: String) => new java.sql.Date(new java.text.SimpleDateFormat("yyyy-MM-dd").parse(s).getTime))
  val valueUDF = udf((s: String) => s.toDouble)

  // Only keep brent with data we know could match ours
  val DF = spark.read.option("header", "true").option("inferSchema", "true").csv("brent.csv").filter(year(col("DATE")) >= 2015)
  DF.show
  DF.createOrReplaceTempView("brent")

  import com.aamend.texata.timeseries.DateUtils.Frequency
  import com.aamend.texata.timeseries.SeriesUtils.FillingStrategy
  import com.aamend.texata.timeseries._

  val trendDF = DF.rdd.map(r => ("DUMMY", Point(r.getAs[java.sql.Date]("DATE").getTime, r.getAs[Double]("VALUE")))).groupByKey().mapValues(it => {
    val series = it.toArray.sortBy(_.x)
    SeriesUtils.completeSeries(series, Frequency.DAY, FillingStrategy.LOCF)
  }).flatMap({ case ((s), series) =>
    new TrendCalculus(series, Frequency.MONTH).getTrends.filter { trend =>
      trend.reversal.isDefined
    }
      .map { trend =>
        (trend.trend.toString, trend.reversal.get.x, trend.reversal.get.y)
      }
      .map { case (trend, x, y) =>
        (trend, new java.sql.Timestamp(x), y)
      }
  }).toDF("trend", "x", "y")

  trendDF.show

  /*
+-----+--------------------+-----+
|trend|                   x|    y|
+-----+--------------------+-----+
|  LOW|2015-01-13 00:00:...|45.13|
| HIGH|2015-05-13 00:00:...|66.33|
|  LOW|2015-08-24 00:00:...|41.59|
| HIGH|2015-10-08 00:00:...|52.13|
|  LOW|2016-01-20 00:00:...|26.01|
| HIGH|2016-06-08 00:00:...|50.73|
|  LOW|2016-08-02 00:00:...| 40.0|
| HIGH|2016-08-26 00:00:...|49.66|
|  LOW|2016-09-27 00:00:...|44.95|
| HIGH|2016-10-19 00:00:...|51.85|
|  LOW|2016-11-13 00:00:...|41.61|
+-----+--------------------+-----+
   */

  val trendUDF = udf((t: String) => if (t == null) "NEUTRAL" else t)
  trendDF.join(DF, trendDF("x") === DF("DATE"), "right_outer")
    .withColumn("TREND", trendUDF(col("trend")))
    .select("DATE", "VALUE", "TREND")
    .createOrReplaceTempView("trends")

}

