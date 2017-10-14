package com.aamend.texata

import java.io.File

import com.aamend.texata.gdelt._
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.sql.SparkSession

class Influencers extends App {

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

  import com.aamend.texata._

  val personRDD = gkgDS.filter(c => {
    c.themes.contains("ENV_GAS") || c.themes.contains("ENV_OIL")
  }).flatMap(f => {
    f.persons.flatMap(p1 => {
      f.persons.filter(p2 => p2 != p1).map(p2 => {
        ((p1, p2), f.numArticles.toLong)
      })
    })
  }).rdd.reduceByKey(_ + _).cache()

  val graph = personRDD.toGraph()
  graph.cache

  graph.vertices.count
  graph.edges.count
  graph.degrees.values.sum / graph.vertices.count

  val subgraph = graph.subgraph(
    (et: EdgeTriplet[String, Long]) => et.attr > 1000,
    (_, vData: String) => true
  )

  val subGraphDeg = subgraph.outerJoinVertices(subgraph.degrees)((vId, vData, vDeg) => {
    (vData, vDeg.getOrElse(0))
  }).subgraph(
    (et: EdgeTriplet[(String, Int), Long]) => et.srcAttr._2 > 0 && et.dstAttr._2 > 0,
    (_, vData: (String, Int)) => vData._2 > 0
  ).mapVertices({ case (vId, (vData, vDeg)) =>
    vData
  })

  val wccGraph = subGraphDeg.wcc().cache()
  val prGraph = wccGraph.outerJoinVertices(wccGraph.pageRank(0.15).vertices)((_, vData, vPr) => {
    (vData._1, vData._2, vPr.getOrElse(0.15))
  })

  prGraph.vertices.values.toDF("person", "community", "pageRank").cache()
  val vertices = prGraph.vertices.values.toDF("person", "community", "pageRank").cache()
  vertices.createOrReplaceTempView("gkg")
  sqlContext.cacheTable("gkg")

  printToFile(new File("edges.csv")) { p =>
    prGraph.edges.map(e => {
      Array(e.srcId, e.dstId, e.attr).mkString(",")
    }).collect().foreach(p.println)
  }

  printToFile(new File("nodes.csv")) { p =>
    prGraph.vertices.map(e => {
      Array(e._1, e._2._1, e._2._2, e._2._3).mkString(",")
    }).collect().foreach(p.println)
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}

