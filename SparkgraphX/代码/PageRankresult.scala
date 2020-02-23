package test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
import test.GraphxExample.graph
import test.jinyongtest.{creatGraph, edgeCount}
object PageRankresult {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def creatGraph(sc:SparkContext,path1:String,path2:String,num:Int) ={
    val hero = sc.textFile(path1)
    val counts = edgeCount(sc,path2,num)
    val verticesAll = hero.map { line =>
      val fields = line.split("\\s+")
      (fields(0).toLong, fields(1))
    }
    val edges = counts.map { line =>
      val fields = line._1.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, line._2)//起始点ID必须为Long，最后一个是属性，可以为任意类型
    }//构造边（基本信息操作）
    val graph_tmp = Graph.fromEdges(edges,1L)
    //    经过过滤后有些顶点是没有边，所以采用leftOuterJoin将这部分顶点去除
    val vertices = graph_tmp.vertices.leftOuterJoin(verticesAll).map(x=>(x._1,x._2._2.getOrElse("")))
    val graph = Graph(vertices,edges)//构造图（基本信息操作）
    graph
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRankTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val graph =creatGraph(sc,"E:\\云计算\\SparkGraphX\\人物\\heromap3.txt",
      "E:\\云计算\\SparkGraphX\\人物关系\\relationship3.txt",15).cache()

    val users = sc.textFile("E:\\云计算\\SparkGraphX\\人物\\heromap3.txt").
      map {
        line =>
          val fields = line.split("\\s+")
          (fields(0).toLong, fields(1))
      }

    val ranks = graph.pageRank(0.001).vertices
    val ranksByUsername = users.join(ranks).map {
      case(id , (username,rank)) => (username,rank)
    }
    graph.degrees.top(10){
      Ordering.by((entry: (VertexId, Int)) => entry._2)
    }.foreach(t => println(t._1 + ": " + t._2))
    ranksByUsername.top(10) {
      Ordering.by((entry: (String, Double)) => entry._2)
    }.foreach(t => println(t._1 + ": " + t._2))

  }
}
