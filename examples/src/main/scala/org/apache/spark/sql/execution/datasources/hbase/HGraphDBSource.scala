/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase

import io.hgraphdb.{HBaseGraph, HBaseGraphConfiguration}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.tinkerpop.gremlin.structure.T
import io.hgraphdb.HBaseGraph
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory
import org.graphframes.GraphFrame

object HGraphDBSource {

  var helper : Map[String, String] = Map("PageRank"->"Usage: PageRank ",
    "PersonalizedPageRank"->"Usage: PersonalizedPageRank <vertexId> ",
    "ConnectedComponents"->"Usage: ConnectedComponents <output>",
    "StronglyConnectedComponents"->"Usage: StronglyConnectedComponents <output>",
    "LabelPropagation"->"Usage: LabelPropagation ",
    "ShortestPaths"->"Usage: ShortestPaths <vertexId...>",
    "TriangleCount"->"Usage: TriangleCount ",
    "SvdPlusPlus"->"Usage: SvdPlusPlus "
  )
  def edgeCatalog(graphName: String) = s"""{
                       |"table":{"namespace":"$graphName", "name":"edges",
                       |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                       |"rowkey":"key",
                       |"columns":{
                       |"id":{"cf":"rowkey", "col":"key", "type":"string"},
                       |"relationship":{"cf":"f", "col":"~l", "type":"string"},
                       |"src":{"cf":"f", "col":"~f", "type":"string"},
                       |"dst":{"cf":"f", "col":"~t", "type":"string"}
                       |}
                       |}""".stripMargin

  def vertexCatalog(graphName: String) = s"""{
                         |"table":{"namespace":"$graphName", "name":"vertices",
                         |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                         |"rowkey":"key",
                         |"columns":{
                         |"id":{"cf":"rowkey", "col":"key", "type":"string"}
                         |}
                         |}""".stripMargin

  def vertexOut(graphName: String) = s"""{
                                            |"table":{"namespace":"$graphName", "name":"vertices",
                                            |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                                            |"rowkey":"key",
                                            |"columns":{
                                            |"id":{"cf":"rowkey", "col":"key", "type":"string"},
                                            |"pagerank":{"cf":"f", "col":"pagerank", "type":"float"},
                                            |"component":{"cf":"f", "col":"component", "type":"long"},
                                            |"label":{"cf":"f", "col":"lpa", "type":"long"},
                                            |"count":{"cf":"f", "col":"tc", "type":"long"},
                                            |"distances":{"cf":"f", "col":"ssp", "type":"string"}
                                            |}
                                            |}""".stripMargin
  def sspVertexOut(graphName: String) = s"""{
                                        |"table":{"namespace":"$graphName", "name":"vertices",
                                        |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                                        |"rowkey":"key",
                                        |"columns":{
                                        |"_1":{"cf":"rowkey", "col":"key", "type":"string"},
                                        |"_2":{"cf":"f", "col":"ssp", "type":"string"}
                                        |}
                                        |}""".stripMargin

  def edgeOut(graphName: String) = s"""{
                                          |"table":{"namespace":"$graphName", "name":"edges",
                                          |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                                          |"rowkey":"key",
                                          |"columns":{
                                          |"id":{"cf":"rowkey", "col":"key", "type":"string"},
                                          |"weight":{"cf":"f", "col":"weight", "type":"string"}
                                          |}
                                          |}""".stripMargin



  def initG() {
    val hconfig = new HBaseGraphConfiguration()
      .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
      .setGraphNamespace("testGraph")
      .setCreateTables(true)
      .set("hbase.zookeeper.quorum", "emr-header-1,emr-header-2,emr-worker-1");

    var graph = GraphFactory.open(hconfig).asInstanceOf[HBaseGraph]
    graph.drop()
    graph = GraphFactory.open(hconfig).asInstanceOf[HBaseGraph]

    val a = graph.addVertex(T.id, "a", "name", "Alice", "age", Int.box(34))
    val b = graph.addVertex(T.id, "b", "name", "Bob", "age", Int.box(36))
    val c = graph.addVertex(T.id, "c", "name", "Charlie", "age", Int.box(30))
    val d = graph.addVertex(T.id, "d", "name", "David", "age", Int.box(29))
    val e = graph.addVertex(T.id, "e", "name", "Esther", "age", Int.box(32))
    val f = graph.addVertex(T.id, "f", "name", "Fanny", "age", Int.box(36))
    val g = graph.addVertex(T.id, "g", "name", "Gabby", "age", Int.box(60))
    a.addEdge("friend", b)
    b.addEdge("follow", c)
    c.addEdge("follow", b)
    f.addEdge("follow", c)
    e.addEdge("follow", f)
    e.addEdge("friend", d)
    d.addEdge("friend", a)
    a.addEdge("friend", e)
    graph.close()
  }

  def doPageRank(g:GraphFrame, graphName:String, zkUrl:String): Unit = {
    // Run PageRank for a fixed number of iterations.
    val pr_results = g.pageRank.resetProbability(0.15).maxIter(10).run()
    pr_results.vertices.write
      .options(Map(
        HBaseTableCatalog.tableCatalog->vertexOut(graphName),
        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    pr_results.edges.write
      .options(Map(
        HBaseTableCatalog.tableCatalog->edgeOut(graphName),
        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

  }

  def doPRPersonalized(g:GraphFrame,vid:String, graphName:String, zkUrl:String): Unit = {
    val pr_results3 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId(vid).run()
//    pr_results3.vertices.select("id", "pagerank").write.csv(output)
    pr_results3.vertices.write
      .options(Map(
        HBaseTableCatalog.tableCatalog->vertexOut(graphName),
        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    pr_results3.edges.write
      .options(Map(
        HBaseTableCatalog.tableCatalog->edgeOut(graphName),
        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  import org.apache.spark.sql.functions.{collect_list}

  def doConnectedComponents(g:GraphFrame, graphName:String, zkUrl:String, output:String, spark:SparkSession): Unit = {
    import spark.implicits._
    val result = g.connectedComponents.run()
    result.select("component","id").groupBy("component").agg(collect_list("id").alias("id"))
      .map(r=> r.getList(1).toString())
      .write.csv(output)
//    result.write
//      .options(Map(
//        HBaseTableCatalog.tableCatalog->vertexOut(graphName),
//        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
//      ))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }

  def stronglyConnectedComponents(g:GraphFrame, graphName:String, zkUrl:String, output:String, spark:SparkSession): Unit = {
    import spark.implicits._
    val result = g.stronglyConnectedComponents.maxIter(10).run()

    result.select("component","id").groupBy("component").agg(collect_list("id").alias("id"))
      .map(r=> r.getList(1).toString())
      .write.csv(output)
//    result2.write
//      .options(Map(
//        HBaseTableCatalog.tableCatalog->vertexOut(graphName),
//        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
//      ))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }

  def labelPropagation(g:GraphFrame, graphName:String, zkUrl:String): Unit = {
    val result3 = g.labelPropagation.maxIter(5).run()
//    result3.select("id", "label").write.csv(output)
    result3.select("id", "label").write
      .options(Map(
        HBaseTableCatalog.tableCatalog->vertexOut(graphName),
        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def writeMap2String(data:scala.collection.Map[String, Integer]) : String  = {
    val sb:StringBuilder = new StringBuilder
    sb.append("{")
    for (entry<- data) {
      sb.append("\"").append(entry._1).append("\"")
      sb.append(":")
      sb.append(entry._2)
      sb.append(",")
    }
    if (data.size > 0) {
      sb.deleteCharAt(sb.length-1)
    }

    sb.append("}")
    sb.toString()
  }

  def shortestPaths(g:GraphFrame, graphName:String, zkUrl:String, vids:Seq[String], sparkSession: SparkSession): Unit = {
    val results = g.shortestPaths.landmarks(vids).run()
    import sparkSession.implicits._
    case class shortestpath(id:String, distances:String)
//    sp_results.select("id",  "distances").map(r=>(r.getString(0),r.getMap(1).toString())).write.csv(output)
    results.select("id",  "distances").map(r=>(r.getString(0),
      writeMap2String(r.getMap[String, Integer](1))
    ))
      .write.options(Map(
      HBaseTableCatalog.tableCatalog->sspVertexOut(graphName),
      HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
    ))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .save()
  }

  def triangleCount(g:GraphFrame, graphName:String, zkUrl:String): Unit = {
    val tc_results = g.triangleCount.run()
//    tc_results.select("id", "count").write.csv(output)
    tc_results.select("id", "count").write
      .options(Map(
        HBaseTableCatalog.tableCatalog->vertexOut(graphName),
        HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""
      ))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  //TODO: not ready
  def svdPlusPlus(g:GraphFrame, graphName:String, zkUrl:String): Unit = {
    g.svdPlusPlus.run()
  }
  def printHelp(): Unit = {
    println("Usage: <hbase-zk-url> <graphname> <algorithm> <algorithm-args>")
    println("<algorithm> <algorithm-args> list belowing")
    helper.values.map((value) => {println(value)})
  }
  def printHelp(cmd:String): Unit = {
    var msg=helper.get(cmd)
    println(msg.get)
  }
  def checkArgs(cmd:String, args: Array[String]):Boolean = {
    val cmdArray = args.drop(2)
    val valid = cmdArray match {
      case Array("PageRank") => true
      case Array("PersonalizedPageRank", _) => true
      case Array("ConnectedComponents", _) => true
      case Array("StronglyConnectedComponents", _) => true
      case Array("LabelPropagation") => true
      case Array("ShortestPaths", _,  _*) => true
      case Array("TriangleCount") => true
      case Array("SvdPlusPlus") => true
      case _ => false
    }
    valid
  }

  def main(args: Array[String]) {
//    initG();

    if (args.length < 3) {
      printHelp();
      System.exit(-1)
    }
    val zkUrl = args(0)
    val graphName = args(1)
    val cmd = args(2)
    if (helper.get(cmd) == None) {
      printHelp();
      System.exit(-1)

    }
    if (!checkArgs(cmd, args)) {
      printHelp(cmd);
      System.exit(-1)
    }
    val spark = SparkSession.builder()
      .appName(graphName + "-" + cmd)
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setCheckpointDir("/tmp")

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat, HBaseRelation.HBASE_CONFIGURATION -> s"""{"hbase.zookeeper.quorum":"$zkUrl"}"""))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val verticesDf = withCatalog(vertexCatalog(graphName))
    val edgesDf = withCatalog(edgeCatalog(graphName))
    val g = GraphFrame(verticesDf, edgesDf)

    val cmdArgs = args.drop(2); //strip zk&graph
    cmdArgs match {
      case Array("PageRank") => doPageRank(g, graphName, zkUrl)
      case Array("PersonalizedPageRank", vid) => doPRPersonalized(g, vid, graphName, zkUrl)
      case Array("ConnectedComponents", output) => doConnectedComponents(g, graphName, zkUrl, output, spark)
      case Array("StronglyConnectedComponents", output) => stronglyConnectedComponents(g, graphName, zkUrl, output, spark)
      case Array("LabelPropagation") => labelPropagation(g, graphName, zkUrl)
      case Array("ShortestPaths", _, _*) =>
        shortestPaths(g, graphName, zkUrl, cmdArgs.drop(1), spark)
      case Array("TriangleCount") => triangleCount(g, graphName, zkUrl)
      case Array("SvdPlusPlus") => svdPlusPlus(g, graphName, zkUrl)
      case _ => Unit
    }

    spark.stop()
  }
}
