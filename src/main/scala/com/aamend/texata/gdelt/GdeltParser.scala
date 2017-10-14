package com.aamend.texata.gdelt

import java.sql.Date
import java.text.SimpleDateFormat

import scala.util.Try

object GdeltParser {

  def parseEvent(row: String): EVENT = {
    val parts = row.split("\t")
    makeEvent(parts)
  }

  protected def makeEvent(parts: Array[String]): EVENT = {
    EVENT(
      eventId = makeLong(parts, 0),
      date = makeDate(parts, 1, "yyyyMMdd"),
      month = makeString(parts, 2),
      year = makeString(parts, 3),
      fractionDate = makeFloat(parts, 4),
      actor1 = makeActor(parts, 5),
      actor2 = makeActor(parts, 15),
      isRootEvent = makeInteger(parts, 25) > 0,
      eventCode = makeString(parts, 26),
      eventCodeStr = cameoEventCodes.getOrElse(makeString(parts, 26), ""),
      eventBaseCode = makeString(parts, 27),
      eventRootCode = makeString(parts, 28),
      quadClass = quadClass(makeInteger(parts, 29)),
      goldsteinScale = makeFloat(parts, 30),
      numMentions = makeInteger(parts, 31),
      numSources = makeInteger(parts, 32),
      numArticles = makeInteger(parts, 33),
      avgTone = makeFloat(parts, 34),
      actor1Geo = makeLocation(parts, 35),
      actor2Geo = makeLocation(parts, 42),
      actionGeo = makeLocation(parts, 49),
      dateAdded = makeLong(parts, 56),
      sourceUrl = makeString(parts, 57)
    )
  }

  protected def makeLong(row: Array[String], i: Int): Long = Try(row(i).toLong).getOrElse(0L)

  protected def makeDate(row: Array[String], i: Int, sdf: String): Date = Try(new Date(new SimpleDateFormat(sdf).parse(row(i)).getTime)).getOrElse(new Date(0L))

  protected def makeInteger(row: Array[String], i: Int): Int = Try(row(i).toInt).getOrElse(0)

  protected def makeFloat(row: Array[String], i: Int): Float = Try(row(i).toFloat).getOrElse(0.0F)

  protected def makeActor(row: Array[String], startIndex: Int): Actor =
    Actor(
      code = makeString(row, startIndex),
      name = makeString(row, startIndex + 1),
      countryCode = makeString(row, startIndex + 2),
      knownGroupCode = makeString(row, startIndex + 3),
      ethnicCode = makeString(row, startIndex + 4),
      religion1Code = makeString(row, startIndex + 5),
      religion2Code = makeString(row, startIndex + 6),
      type1Code = makeString(row, startIndex + 7),
      type2Code = makeString(row, startIndex + 8),
      type3Code = makeString(row, startIndex + 9)
    )

  protected def makeString(row: Array[String], i: Int): String = Try(row(i)).getOrElse("")

  protected def makeLocation(a: Array[String], startingIndex: Int = 0): Location = {
    val cc = makeString(a, startingIndex + 2)
    Location(
      `type` = locationType(makeInteger(a, startingIndex)),
      fullName = makeString(a, startingIndex + 1),
      countryCode = cc,
      countryToName.getOrElse(cc, ""),
      adm1Code = makeString(a, startingIndex + 3),
      latitude = makeFloat(a, startingIndex + 4),
      longitude = makeFloat(a, startingIndex + 5),
      featureID = makeInteger(a, startingIndex + 6)
    )
  }

  def parseGkg(row: String): GKG = {
    makeGkg(row.split("\t"))
  }

  protected def makeArrayString(a: Array[String], i: Int, delimiter: String): Array[String] = {
    Try(a(i).split(delimiter)).getOrElse(Array.empty[String])
  }

  protected def makeArrayLong(a: Array[String], i: Int, delimiter: String): Array[Long] = {
    Try(a(i).split(delimiter)).getOrElse(Array.empty[String]).map(l => Try(l.toLong)).filter(_.isSuccess).map(_.get)
  }

  protected def makeArrayLocation(root: Array[String], i: Int): Array[Location] = {
    Try(root(i).split(";")).getOrElse(Array.empty[String]).map(string => {
      val a = string.split("#")
      makeLocation(a)
    })
  }

  protected def makeTone(root: Array[String], i: Int = 0): Tone = {
    val a = Try(root(7).split(",")).getOrElse(Array.empty[String])
    Tone(
      tone = makeFloat(a, 0),
      positive = makeFloat(a, 1),
      negative = makeFloat(a, 2),
      polarity = makeFloat(a, 3),
      activityReferenceDensity = makeFloat(a, 4),
      groupReferenceDensity = makeFloat(a, 5)
    )
  }

  protected def makeGkg(root: Array[String]): GKG = {
    GKG(
      date = makeDate(root, 0, "yyyyMMdd"),
      numArticles = makeInteger(root, 1),
      counts = makeArrayCount(root, 2),
      themes = makeArrayString(root, 3, ";"),
      locations = makeArrayLocation(root, 4),
      persons = makeArrayString(root, 5, ";"),
      organizations = makeArrayString(root, 6, ";"),
      tone = makeTone(root, 7),
      eventIds = makeArrayLong(root, 8, ","),
      sources = makeArrayString(root, 9, ";"),
      sourceUrls = makeArrayString(root, 10, "<UDIV>")
    )
  }

  protected def makeArrayCount(array: Array[String], i: Int): Array[Count] = {
    Try(array(i).split(";")).getOrElse(Array.empty[String]).map(count => {
      val a = count.split("#")
      makeCount(a)
    })
  }

  protected def makeCount(array: Array[String]): Count = {
    Count(
      `type` = makeString(array, 0),
      number = makeInteger(array, 1),
      objectType = makeString(array, 2),
      location = makeLocation(array.drop(3))
    )
  }

}

