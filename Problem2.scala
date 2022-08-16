package comp9313.proj2
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Problem2 {
	def main(args: Array[String]) = {
    		
    		//input files
    		val outputFolder = args(1)
		val inputFile = args(0)

		//initialise spark
		val conf = new SparkConf().setAppName("Problem2").setMaster("local")
		val sc = new SparkContext(conf)
		val data = sc.textFile(inputFile)

		//split the given file into lines and split the lines to make fromNode, toNode, and Weight
		val lines = data.flatMap { x => x.split("\n") }
		val edgeRDD = lines.map(x => x.split(" ")).map { x => Edge(x(1).toLong, x(2).toLong, 1.0) }
		edgeRDD.foreach{println}

		//construct the graph by the edge
		val graph = Graph.fromEdges(edgeRDD, 1)

		var finalList  = ListBuffer[(Int)]()
		//for each src node we find all nodes reachable and print this amount
      		for (i <- 0 to graph.numVertices.toInt - 1) {
      		
      	 		// dijkstra's algo found in lecture notes 5.2 slide 51
			val startGraph = graph.mapVertices((id, _) => if(id==i) 500.0 else Double.PositiveInfinity)		// count src if cycle
		 	val sssp = startGraph.pregel(Double.PositiveInfinity)(
		  		(id, dist, newDist) => math.min(dist, newDist), 
		  		triplet =>{
			    		if(triplet.srcAttr + triplet.attr < triplet.dstAttr)
				    	{
				      		Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
				    	}else{
				      		Iterator.empty
				    	}
			  	}, 
		  		(a, b) => math.max(a, b)
			 )
			 
			//if there is a edge between two nodes 
			val num = sssp.vertices.map(x => if(x._2!=Double.PositiveInfinity&&x._2!=500.0)(i,1)else(i,0)).reduceByKey(_+_) // change ?
			// convert to list of all nodes reachable
 			val res = num.map(x => x._2).collect().toList
			for(j <- 0 to res.length - 1){
				finalList.append(res(j))
			}
		}
		// for all nodes we print the src node and number of nodes reachable 
		var finalRDDList  = ListBuffer[(String)]()
		for(i <- 0 to finalList.length - 1){
			finalRDDList.append(i + ":" + finalList(i))
			//println(i + ":" + finalList(i))
		}  
		//Convert list to RDD
		val finalRDD = sc.parallelize(finalRDDList)
		finalRDD.saveAsTextFile(outputFolder)
	}
}

