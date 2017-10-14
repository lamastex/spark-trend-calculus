package com.aamend.texata

import java.sql.Date

import com.aamend.texata.gdelt.GdeltParser._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, Dataset}

import scala.io.Source

package object gdelt {

  lazy val countryToName: Map[String, String] = Source
    .fromInputStream(this.getClass.getResourceAsStream("/country.dat"))
    .getLines()
    .map(
      s => {
        val a = s.split("\t")
        (a(3), a(4))
      }
    ).toMap

  lazy val cameoEventCodes: Map[String, String] = Source
    .fromInputStream(this.getClass.getResourceAsStream("/cameo.csv"))
    .getLines()
    .map(
      s => {
        val Array(key, value) = s.split(",", 2)
        (key, value.toLowerCase().trim)
      }
    ).toMap

  implicit class GdeltParserSparkContext(sc: SparkContext) {
    def gdeltGKG(s: String): RDD[GKG] = {
      sc.textFile(s).gdeltGKG
    }

    def gdeltEVENT(s: String): RDD[EVENT] = {
      sc.textFile(s).gdeltEVENT
    }
  }

  implicit class GdeltParserSparkContextRDD(rdd: RDD[String]) {
    def gdeltGKG: RDD[GKG] = {
      rdd.filter(!_.startsWith("DATE")).map(parseGkg)
    }

    def gdeltEVENT: RDD[EVENT] = {
      rdd.map(parseEvent)
    }
  }

  implicit class GdeltParserSparkSession(dfr: DataFrameReader) {
    def gdeltGKG(s: String): Dataset[GKG] = {
      val ds = dfr.textFile(s)
      ds.gdeltGKG
    }

    def gdeltEVENT(s: String): Dataset[EVENT] = {
      val ds = dfr.textFile(s)
      ds.gdeltEVENT
    }
  }

  implicit class GdeltParserSparkSessionDS(ds: Dataset[String]) {

    import ds.sparkSession.implicits._

    def gdeltGKG: Dataset[GKG] = {
      ds.filter(!_.startsWith("DATE")).map(parseGkg)
    }

    def gdeltEVENT: Dataset[EVENT] = {
      ds.map(parseEvent)
    }
  }

  def locationType(i: Int): String = i match {
    case 1 => "COUNTRY"
    case 2 => "USSTATE"
    case 3 => "USCITY"
    case 4 => "WORLDCITY"
    case 5 => "WORLDSTATE"
    case _ => ""
  }

  def quadClass(i: Int): String = i match {
    case 1 => "Verbal Cooperation"
    case 2 => "Material Cooperation"
    case 3 => "Verbal Conflict"
    case 4 => "Material Conflict"
    case _ => ""
  }

  case class Count(
                    `type`: String,
                    number: Int,
                    objectType: String,
                    location: Location
                  )

  case class Tone(
                   tone: Float,
                   positive: Float,
                   negative: Float,
                   polarity: Float,
                   activityReferenceDensity: Float,
                   groupReferenceDensity: Float
                 )

  case class GKG(
                  date: Date,
                  numArticles: Int,
                  counts: Array[Count],
                  themes: Array[String],
                  locations: Array[Location],
                  persons: Array[String],
                  organizations: Array[String],
                  tone: Tone,
                  eventIds: Array[Long],
                  sources: Array[String],
                  sourceUrls: Array[String]
                )

  case class Actor(
                    code: String,
                    name: String,
                    countryCode: String,
                    knownGroupCode: String,
                    ethnicCode: String,
                    religion1Code: String,
                    religion2Code: String,
                    type1Code: String,
                    type2Code: String,
                    type3Code: String
                  )

  case class Location(
                       `type`: String,
                       fullName: String,
                       countryCode: String,
                       countryName: String,
                       adm1Code: String,
                       latitude: Float,
                       longitude: Float,
                       featureID: Int
                     )

  case class EVENT(
                    eventId: Long,
                    date: Date,
                    month: String,
                    year: String,
                    fractionDate: Float,
                    actor1: Actor,
                    actor2: Actor,
                    isRootEvent: Boolean,
                    eventCode: String,
                    eventCodeStr: String,
                    eventBaseCode: String,
                    eventRootCode: String,
                    quadClass: String,
                    goldsteinScale: Float,
                    numMentions: Int,
                    numSources: Int,
                    numArticles: Int,
                    avgTone: Float,
                    actor1Geo: Location,
                    actor2Geo: Location,
                    actionGeo: Location,
                    dateAdded: Long,
                    sourceUrl: String
                  )

}
