package com.aamend.texata

import java.sql.Date

import com.aamend.texata.gdelt._
import com.aamend.texata.timeseries.DateUtils.Frequency
import com.aamend.texata.timeseries.SeriesUtils.FillingStrategy
import com.aamend.texata.timeseries.{DateUtils, Point, SeriesUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{window, _}

class BreakingNewsExtractor extends App {

  val spark = SparkSession.builder().appName("gdelt-harness").getOrCreate()
  val sqlContext = spark.sqlContext

  import spark.implicits._

  // Mock up zeppelin
  val z = Map.empty[String, String]

  val inputGkg = "s3://texata-round2/gdelt/gkg"
  val inputEvent = "s3://texata-round2/gdelt/events"

  val eventDS = spark.read.gdeltEVENT(inputEvent)
  val gkgDS = spark.read.gdeltGKG(inputGkg)

  // I extract all Event that are about OIL and GAS (thx to the GKG theme)

  val oilGKGDF = gkgDS.filter(c => {
    c.themes.contains("ENV_GAS") || c.themes.contains("ENV_OIL")
  }).flatMap(c => {
    c.eventIds.map(_ -> "DUMMY")
  }).toDF("eventId", "_dummy")
  val oilEventSeries = oilEventDF.groupBy(
    col("actionGeo.countryCode"),
    col("date")
  )
    .agg(
      sum(col("numArticles")).as("articles"),
      avg(col("goldsteinScale")).as("goldstein")
    )
    .rdd.map(r => {
    (r.getAs[String]("countryCode"), Point(r.getAs[Date]("date").getTime, r.getAs[Int]("articles").toDouble))
  })
    .groupByKey()
    .mapValues(_.toArray.sortBy(_.x))
    .mapValues(p => SeriesUtils.completeSeries(p, Frequency.DAY, FillingStrategy.LINEAR))
    .mapValues(p => SeriesUtils.normalizeSeries(p))
    .flatMap({ case (country, ps) =>
      val weekly = movingAverage(ps, Frequency.WEEK)
      weekly.map(p =>
        (country, new Date(p.x), p.y))
    }).toDF("country", "date", "articles").cache()

  // Checkpoint here
  oilEventDF.write.parquet("oil_events")
  oilEventDF = spark.read.parquet("oil_events")

  // Let's get some graph
  val mve = oilEventSeries.filter(!col("articles").isNaN)
    .groupBy(window(col("date"), "1 week"), col("country"))
    .agg(avg("articles").as("articles"))
    .orderBy(desc("articles"))
    .select(col("window.start").as("date"), col("country"), col("articles"))
    .limit(10000)

  val df = spark.read.parquet("oil_events").withColumnRenamed("date", "_date")


  oilEventSeries.createOrReplaceTempView("oil_gas")
  sqlContext.cacheTable("oil_gas")

  //TODO: See screenshot FR_UK_OIL-events

  // I extract the top events in the OIL and GAS
  val mveEvents = mve.join(df, df("_date") === mve("date") && df("actionGeo.countryCode").as("_country") === mve("country")).cache()

  mve.show()

  /*
+-------------------+-------+------------------+
|               date|country|          articles|
+-------------------+-------+------------------+
|2016-03-03 00:00:00|     NC|13.662072684496462|
|2016-04-14 00:00:00|     MJ|13.414666979122172|
|2016-02-25 00:00:00|     NC|12.405574670546713|
|2014-09-25 00:00:00|     SU|  9.48863214818537|
|2015-07-30 00:00:00|     GA|  9.29919393241244|
|2016-01-21 00:00:00|     SZ| 8.868822058237168|
|2016-07-07 00:00:00|     FJ| 8.484678646900855|
|2017-08-03 00:00:00|     VE|  8.31103934678676|
|2017-07-27 00:00:00|     VE|  8.02805049313106|
|2017-02-23 00:00:00|     MY| 8.022447901213992|
|2015-03-12 00:00:00|     MP| 8.000639172377282|
|2016-04-07 00:00:00|     MJ| 7.972090458950071|
|2016-02-25 00:00:00|     IV| 7.945543203860656|
|2015-07-09 00:00:00|     EC|7.7897667107480775|
|2016-05-05 00:00:00|     CA|  7.56970466851033|
|2015-01-15 00:00:00|     MC| 7.471498021926732|
|2016-01-14 00:00:00|     IR|7.2391919471156125|
|2016-07-14 00:00:00|     CH| 7.145798060874983|
|2016-07-14 00:00:00|     RP| 7.072568219684949|
|2015-07-23 00:00:00|     GA| 6.950294549668176|
+-------------------+-------+------------------+
   */


  mve.rdd.map(r => {
    (r.getAs[String]("country"), r.getAs[Date]("date"), r.getAs[Double]("articles"))
  })
  val urlRDD = mveEvents.select("sourceUrl").distinct().rdd.map(_.getAs[String]("sourceUrl"))
  val articleDF = urlRDD.distinct.mapPartitions(fetcher).toDF("_URL", "title", "description", "text", "tags")
  val mveEnrichedDF = mveEvents.join(articleDF, col("_URL") === col("sourceURL")).orderBy(col("date")).select("date", "country", "goldsteinScale", "sourceUrl", "title", "description", "tags")

  import com.gravity.goose.{Configuration, Goose}

  var oilEventDF = eventDS.toDF().join(oilGKGDF, "eventId")

  def movingAverage(timeseries: Array[Point], grouping: Frequency.Value): Array[Point] = {
    timeseries.groupBy(p => DateUtils.roundTime(grouping, p.x)).flatMap({ case (group, groupedSeries) =>
      val avg = groupedSeries.map(_.y).sum / groupedSeries.length
      groupedSeries.map(p => {
        Point(p.x, avg)
      })
    }).toArray.sortBy(_.x)
  }

  def fetcher(iterator: Iterator[String]): Iterator[(String, String, String, String, Array[_ <: String])] = {
    val conf: Configuration = new Configuration
    conf.setBrowserUserAgent("mozilla texata")
    conf.setEnableImageFetching(false)
    conf.setConnectionTimeout(12000)
    conf.setSocketTimeout(12000)
    val goose = new Goose(conf)
    for (url <- iterator) yield {
      try {
        val article = goose.extractContent(url)
        (
          url,
          article.title,
          article.metaDescription,
          article.cleanedArticleText,
          article.tags.toSet.toArray
        )
      } catch {
        case _: Throwable => (
          url,
          null,
          null,
          null,
          Array.empty[String]
        )
      }
    }
  }

  mveEnrichedDF.cache()
  mveEnrichedDF.show()

  mveEnrichedDF.filter(col("title").isNotNull && length(col("title")) > 0).rdd.map(r => {
    (r.getAs[String]("country"), (r.getAs[Double]("goldsteinScale"), r.getAs[String]("title"), r.getAs[String]("description"), r.getAs[Date]("date")))
  }).groupByKey().mapValues(_.toList.maxBy(v => math.abs(v._1))).map({ case (country, (goldstein, title, description, date)) =>
    (country, title, description, date)
  }).toDF("country", "title", "description", "date").show()

}
