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
class DSV (var line:String="", var delimiter:String=",",var parts:Array[String]=Array("")) extends Serializable {
        parts=line.split(delimiter,-1)
        def hasValidVal(index: Int):Boolean={
                return (parts(index)!=null)&&(parts.length>index)
        }
        def contains(text:String):Boolean={
                for(i <- 1 to (parts.length-1))
                        if(parts(i).contains(text))
                                return false
                true
        }
        override def toString():String={
                var rep:String=""
                for(i <- 0 to (parts.length-1)){
                        rep=rep+parts(i)
                        if (i!=(parts.length -1))
                                rep=rep+","
                }
                rep=rep+"\n"
                rep
        }
}

var month="0603"//get it as command line arg

var districts=sc.textFile("Rwanda_In/Districts.csv").filter(line=>(line.contains("ID")==false)).map(line=>(new DSV(line,","))).map(d=>(d.parts(1))).distinct.collect()

districts.foreach(d=>({

val graph = GraphLoader.edgeListFile(sc, "Rwanda_In/NetworkGraph/EdgeList"+month+"-"+d+".csv", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
//The documentation says that the canonical orientation must be true for connected components algorithm but this thread says that it is an overstatement
//http://mail-archives.apache.org/mod_mbox/spark-dev/201501.mbox/%3CCAPh_B=aiXpoeRAnHs1K6vZRiEqg_fmSMhr5rudqvvF6hzVRraw@mail.gmail.com%3E
//Basically this thread says that if there are more than one edges between the two vertices then the srcId<destId
//To ensure this thing, the python script undirectionality_enforcer should be used

val triCounts = graph.triangleCount().vertices

triCounts.saveAsTextFile("Rwanda_Out/TriangleCounts/Triangle_Counting-"+month+"-"+d+".csv")

}))

//districts.foreach(d=>({

val graph = GraphLoader.edgeListFile(sc, "Rwanda_In/NetworkGraph/EdgeList"+month+"-KigaliNetwork.csv", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
//The documentation says that the canonical orientation must be true for connected components algorithm but this thread says that it is an overstatement
//http://mail-archives.apache.org/mod_mbox/spark-dev/201501.mbox/%3CCAPh_B=aiXpoeRAnHs1K6vZRiEqg_fmSMhr5rudqvvF6hzVRraw@mail.gmail.com%3E
//Basically this thread says that if there are more than one edges between the two vertices then the srcId<destId
//To ensure this thing, the python script undirectionality_enforcer should be used

val triCounts = graph.triangleCount().vertices

triCounts.saveAsTextFile("Rwanda_Out/TriangleCounts/Triangle_Counting-"+month+"-network-kigali.csv")

//}))
