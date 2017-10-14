package com.aamend.texata.gdelt

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.io.Source

class GdeltParserTest extends FunSuite with Matchers with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "Gdelt-parser"

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  private def readDataset(resource: String): Dataset[String] = {
    val ss = Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().toList
    val sparkSession = spark.emptyDataFrame.sparkSession
    import sparkSession.implicits._
    spark.sparkContext.parallelize(ss).toDS()
  }

  test("Parsing Gdelt EVENT") {
    readDataset("gdelt-event.csv").gdeltEVENT.show()
  }

  test("Parsing Gdelt GKG") {
    readDataset("gdelt-gkg.csv").gdeltGKG.show()
  }

  test("Get Cameo") {
    val sparkSession = spark.emptyDataFrame.sparkSession
    import sparkSession.implicits._
    cameoEventCodes.toList.toDF("code", "name").show()
  }

}
