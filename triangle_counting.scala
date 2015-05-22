import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import math.abs


//input should be tab seperated with numeric ids like this so L prefix has to be stripped	
/*
95013926        34805152
38373629        31373811
14988215        99483139
40693681        29253944
*/



val graph = GraphLoader.edgeListFile(sc, "Rwanda_Out/EdgeList_hd-0604.csv", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
//The documentation says that the canonical orientation must be true for connected components algorithm but this thread says that it is an overstatement
//http://mail-archives.apache.org/mod_mbox/spark-dev/201501.mbox/%3CCAPh_B=aiXpoeRAnHs1K6vZRiEqg_fmSMhr5rudqvvF6hzVRraw@mail.gmail.com%3E
//Basically this thread says that if there are more than one edges between the two vertices then the srcId<destId
//To ensure this thing, the python script undirectionality_enforcer should be used

val triCounts = graph.triangleCount().vertices

triCounts.saveAsTextFile("Rwanda_Out/Triangle_Counting-0604-hd.csv")


val graph = GraphLoader.edgeListFile(sc, "Rwanda_Out/EdgeList_hp-0604.csv", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices

triCounts.saveAsTextFile("Rwanda_Out/Triangle_Counting-0604-hp.csv")



val graph = GraphLoader.edgeListFile(sc, "Rwanda_Out/EdgeList_kg-0604.csv", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices

triCounts.saveAsTextFile("Rwanda_Out/Triangle_Counting-0604-kg.csv")


val graph = GraphLoader.edgeListFile(sc, "Rwanda_Out/EdgeList_nkg-0604.csv", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices

triCounts.saveAsTextFile("Rwanda_Out/Triangle_Counting-0604-nkg.csv")
