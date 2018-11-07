usage() {
cat <<EOF
Usage: $0 <spark-master> <hbase-zk> <graphName>  <algorithm> <algorithm-args> 
algorithm:
		PageRank
		PersonalizedPageRank <vertexId>
		ConnectedComponents <output> //output stored to hdfs filesystem
		StronglyConnectedComponents <output> //output stored to hdfs filesystem
		LabelPropagation
		ShortestPaths  <vertexId...> //vertexId... 指定单源序列，查询所有其他顶点到此顶点距离
		TriangleCount
		SvdPlusPlus 
EOF
    exit 1
}
pushd . > /dev/null
sDir=$(dirname "$0")
cd "${sDir}" || usage
sDir=$(pwd -P)
popd > /dev/null

if [ -z "$SPARK_HOME" ]; then
    echo "Please set SPARK_HOME first, export SPARK_HOME=..."
    exit 1
fi

echo "SPARK_HOME:$SPARK_HOME"
if [[ $# < 4 ]]; then
	usage
fi

MASTER=$1
ZK=$2
GRAPH=$3
CMD=$4
shift
shift
shift
case $CMD in
    PageRank)
        ;;
    PersonalizedPageRank)
        ;;
    ConnectedComponents)
        ;;
    StronglyConnectedComponents)
        ;;
    LabelPropagation)
        ;;
    ShortestPaths)
        ;;
    TriangleCount)
        ;;
    SvdPlusPlus)
        ;;
    *)
        usage      # unknown option
        ;;
esac
MLJar=$(find "${sDir}"/examples/target -name 'shc-examples*.jar' 2>/dev/null | sort -r | head -1)
if [ -z "${MLJar}" ]; then
    echo "Please run 'mvn package' to build the project first"
    exit 1
fi

LIB=lib/com.aliyun.hbase_alihbase-annotations-1.1.3.jar,lib/com.aliyun.hbase_alihbase-client-1.1.3.jar,lib/com.aliyun.hbase_alihbase-common-1.1.3.jar,lib/com.aliyun.hbase_alihbase-protocol-1.1.3.jar,lib/com.carrotsearch_hppc-0.7.1.jar,lib/com.esotericsoftware_kryo-4.0.2.jar,lib/com.esotericsoftware_minlog-1.3.0.jar,lib/com.esotericsoftware_reflectasm-1.11.3.jar,lib/com.github.stephenc.findbugs_findbugs-annotations-1.3.9-1.jar,lib/com.google.cloud.bigtable_bigtable-hbase-1.x-shaded-1.3.0.jar,lib/com.google.code.findbugs_jsr305-3.0.2.jar,lib/com.google.guava_guava-12.0.1.jar,lib/com.google.protobuf_protobuf-java-2.5.0.jar,lib/com.jcabi_jcabi-log-0.14.jar,lib/com.jcabi_jcabi-manifests-1.1.jar,lib/com.squareup_javapoet-1.8.0.jar,lib/commons-codec_commons-codec-1.9.jar,lib/commons-collections_commons-collections-3.2.2.jar,lib/commons-configuration_commons-configuration-1.10.jar,lib/commons-io_commons-io-2.4.jar,lib/commons-lang_commons-lang-2.6.jar,lib/commons-logging_commons-logging-1.2.jar,lib/graphframes-0.6.0-spark2.3-s_2.11.jar,lib/io.dropwizard.metrics_metrics-core-3.1.2.jar,lib/io.hgraphdb_hgraphdb-2.0.0.jar,lib/io.netty_netty-all-4.0.23.Final.jar,lib/log4j_log4j-1.2.17.jar,lib/net.bytebuddy_byte-buddy-1.8.3.jar,lib/net.bytebuddy_byte-buddy-agent-1.8.3.jar,lib/net.objecthunter_exp4j-0.4.8.jar,lib/org.apache.htrace_htrace-core-3.1.0-incubating.jar,lib/org.apache.tinkerpop_gremlin-core-3.3.2.jar,lib/org.apache.tinkerpop_gremlin-shaded-3.3.2.jar,lib/org.apache.zookeeper_zookeeper-3.4.6.jar,lib/org.codehaus.jackson_jackson-core-asl-1.9.13.jar,lib/org.codehaus.jackson_jackson-mapper-asl-1.9.13.jar,lib/org.hamcrest_hamcrest-core-1.3.jar,lib/org.javatuples_javatuples-1.2.jar,lib/org.jruby.jcodings_jcodings-1.0.8.jar,lib/org.jruby.joni_joni-2.1.2.jar,lib/org.mortbay.jetty_jetty-util-6.1.26.jar,lib/org.objenesis_objenesis-2.6.jar,lib/org.ow2.asm_asm-5.0.4.jar,lib/org.slf4j_jcl-over-slf4j-1.7.21.jar,lib/org.slf4j_slf4j-api-1.7.21.jar,lib/org.slf4j_slf4j-log4j12-1.6.1.jar,lib/org.yaml_snakeyaml-1.15.jar,lib/scala-logging-api_2.11-2.1.2.jar,lib/scala-logging-slf4j_2.11-2.1.2.jar,lib/shc-core-1.1.2-2.3-s_2.11-SNAPSHOT.jar
#/Users/chenjiang/bin/spark-2.3.1-bin-hadoop2.7/bin/spark-submit --executor-memory 6G --executor-cores 1 --total-executor-cores 40 --conf spark.default.parallelism=120 --master local  --jars $LIB --class org.apache.spark.sql.execution.datasources.hbase.HGraphDBSource lib/shc-examples-1.1.2-2.3-s_2.11-SNAPSHOT.jar emr-header-1,emr-header-2,emr-worker-1 testGraph "$@" 
$SPARK_HOME/bin/spark-submit --executor-cores 1 --master $MASTER --jars $LIB --class org.apache.spark.sql.execution.datasources.hbase.HGraphDBSource $MLJar $ZK $GRAPH "$@" 
