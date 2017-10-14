package com.aamend

import java.nio.charset.Charset

import com.aamend.texata.graph.louvain.LouvainDetection
import com.aamend.texata.graph.wcc.WCCDetection
import com.google.common.hash.Hashing
import org.apache.commons.lang.StringUtils
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexRDD}
import org.apache.spark.rdd.RDD

package object texata {

  implicit class VertexStringImpl(s: String) {
    def toVertex: Long = {
      Hashing.sha512().hashBytes(s.getBytes(Charset.defaultCharset())).asLong()
    }
  }

  implicit class VertexStringGraph(rdd: RDD[((String, String), Long)]) {
    def toGraph(minWeight: Long = Long.MinValue): Graph[String, Long] = {
      val vertices = rdd.keys.keys.union(rdd.keys.values).distinct().sortBy(l => l).map(s => (s.toVertex, s))
      val edges = rdd.map({ case ((from, to), count) =>
        Edge(from.toVertex, to.toVertex, count)
      })
      Graph.apply(vertices, edges).subgraph(
        (et: EdgeTriplet[String, Long]) => et.attr > minWeight,
        (_, vData) => StringUtils.isNotEmpty(vData)
      ).cache()
    }
  }

  implicit class GraphImpl(g: Graph[String, Long]) {
    def wcc(partitions: Int = 100): Graph[(String, Long), Long] = {
      val communityDetection = new WCCDetection(partitions = partitions)
      val vertices: VertexRDD[Long] = communityDetection.run[String](g)
      g.outerJoinVertices(vertices)((vId, vData, vC) => {
        (vData, vC.getOrElse(vId))
      })
    }

    def louvain(minProgress: Int = 1, progressCounter: Int = 1): Graph[(String, Long), Long] = {
      val communityDetection = new LouvainDetection(minProgress, progressCounter)
      val vertices: VertexRDD[Long] = communityDetection.run[String](g)
      g.outerJoinVertices(vertices)((vId, vData, vC) => {
        (vData, vC.getOrElse(vId))
      })
    }
  }


}
