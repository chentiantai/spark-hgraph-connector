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
 */

package org.apache.spark.sql

import io.hgraphdb.{HBaseGraph, HBaseGraphConfiguration}
import org.apache.spark.sql.functions.{col, lit, sum, when}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, Logging}
import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages

class HGraphDBSuite extends SHC with Logging {
  def edgeCatalog = s"""{
                    |"table":{"namespace":"testGraph", "name":"edges",
                    |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                    |"rowkey":"key",
                    |"columns":{
                    |"id":{"cf":"rowkey", "col":"key", "type":"string"},
                    |"relationship":{"cf":"f", "col":"~l", "type":"string"},
                    |"src":{"cf":"f", "col":"~f", "type":"string"},
                    |"dst":{"cf":"f", "col":"~t", "type":"string"}
                    |}
                    |}""".stripMargin

  def vertexCatalog = s"""{
                       |"table":{"namespace":"testGraph", "name":"vertices",
                       |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
                       |"rowkey":"key",
                       |"columns":{
                       |"id":{"cf":"rowkey", "col":"key", "type":"string"},
                       |"name":{"cf":"f", "col":"name", "type":"string"},
                       |"age":{"cf":"f", "col":"age", "type":"int"}
                       |}
                       |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("populate graph") {
    val hconfig = new HBaseGraphConfiguration(htu.getConfiguration())
      .setGraphNamespace("testGraph")
      .setCreateTables(true)
    val conn = htu.getConnection()
    val graph = new HBaseGraph(hconfig, conn)
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

  test("all queries from GraphFrames User Guide") {
    val verticesDf = withCatalog(vertexCatalog)
    val edgesDf = withCatalog(edgeCatalog)
    val g = GraphFrame(verticesDf, edgesDf)

    // Display the vertex and edge DataFrames
    g.vertices.show()
    g.edges.show()

    // Get a DataFrame with columns "id" and "inDeg" (in-degree)
//    g.inDegrees.show()
//
//    // Find the youngest user's age in the graph.
//    // This queries the vertex DataFrame.
//    g.vertices.groupBy().min("age").show()
//
//    // Count the number of "follows" in the graph.
//    // This queries the edge DataFrame.
//    val numFollows = g.edges.filter("relationship = 'follow'").count()
//    println("Number of followers " + numFollows)
//
//    // Search for pairs of vertices with edges in both directions between them.
//    val motifs: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
//    motifs.show()
//
//    // More complex queries can be expressed by applying filters.
//    motifs.filter("b.age > 30").show()
//
//    // Find chains of 4 vertices.
//    val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
//
//    // Query on sequence, with state (cnt)
//    //  (a) Define method for updating state given the next element of the motif.
//    def sumFriends(cnt: Column, relationship: Column): Column = {
//      when(relationship === "friend", cnt + 1).otherwise(cnt)
//    }
//    //  (b) Use sequence operation to apply method to sequence of elements in motif.
//    //      In this case, the elements are the 3 edges.
//    val condition = Seq("ab", "bc", "cd").
//      foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship")))
//    //  (c) Apply filter to DataFrame.
//    val chainWith2Friends2 = chain4.where(condition >= 2)
//    chainWith2Friends2.show()
//
//    // Select subgraph of users older than 30, and edges of type "friend"
//    val v2 = g.vertices.filter("age > 30")
//    val e2 = g.edges.filter("relationship = 'friend'")
//    val g2 = GraphFrame(v2, e2)
//
//    // Select subgraph based on edges "e" of type "follow"
//    // pointing from a younger user "a" to an older user "b".
//    val paths = g.find("(a)-[e]->(b)")
//      .filter("e.relationship = 'follow'")
//      .filter("a.age < b.age")
//    // "paths" contains vertex info. Extract the edges.
//    val e3 = paths.select("e.src", "e.dst", "e.relationship")
//    // In Spark 1.5+, the user may simplify this call:
//    //  val e2 = paths.select("e.*")
//
//    // Construct the subgraph
//    val g3 = GraphFrame(g.vertices, e3)
//
//    // Search from "Esther" for users of age <= 32.
//    val paths2: DataFrame = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").run()
//    paths2.show()
//
//    // Specify edge filters or max path lengths.
//    g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
//      .edgeFilter("relationship != 'friend'")
//      .maxPathLength(3)
//      .run()

    sc.setCheckpointDir("/tmp")
    println("===cal connectedComponents")

    val result = g.connectedComponents.setAlgorithm("graphx").run()
    result.select("id", "component").orderBy("component").show()
//    result.write.csv("/tmp/connectedComponents")

    println("===cal stronglyConnectedComponents")
    val result2 = g.stronglyConnectedComponents.maxIter(3).run()
    result2.select("id", "component").orderBy("component").show()
    result2.write.csv("/tmp/stronglyConnectedComponents")

    println("===cal labelPropagation")
    val result3 = g.labelPropagation.maxIter(5).run()
    result3.select("id", "label").show()
//    result3.write.csv("/tmp/labelPropagation")


    //    // Run PageRank until convergence to tolerance "tol".
//    val pr_results = g.pageRank.resetProbability(0.15).tol(0.01).run()
//    pr_results.vertices.select("id", "pagerank").show()
//    pr_results.edges.select("src", "dst", "weight").show()
//
//    // Run PageRank for a fixed number of iterations.
//    val pr_results2 = g.pageRank.resetProbability(0.15).maxIter(10).run()
//
//    // Run PageRank personalized for vertex "a"
//    val pr_results3 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()

//    logInfo("===cal shortestPaths between a and d")
//    val sp_results = g.shortestPaths.landmarks(Seq("a", "d")).run()
//    sp_results.select("id", "distances").show()
//
//    logInfo("===cal triangleCount between a and d")
//    val tc_results = g.triangleCount.run()
//    tc_results.select("id", "count").show()
//
//    // Save vertices and edges as Parquet to some location.
//    g.vertices.write.parquet("/tmp/vertices")
//    g.edges.write.parquet("/tmp/edges")
//
//    // Load the vertices and edges back.
//    val sameV = sqlContext.read.parquet("/tmp/vertices")
//    val sameE = sqlContext.read.parquet("/tmp/edges")
//
//    // Create an identical GraphFrame.
//    val sameG = GraphFrame(sameV, sameE)
//
//    // We will use AggregateMessages utilities later, so name it "AM" for short.
//    val AM = AggregateMessages
//
//    // For each user, sum the ages of the adjacent users.
//    val msgToSrc = AM.dst("age")
//    val msgToDst = AM.src("age")
//    val agg = g.aggregateMessages
//      .sendToSrc(msgToSrc)  // send destination user's age to source
//      .sendToDst(msgToDst)  // send source user's age to destination
//      .agg(sum(AM.msg).as("summedAges"))  // sum up ages, stored in AM.msg column
//    agg.show()
  }
}
