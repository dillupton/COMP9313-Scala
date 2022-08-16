package comp9313.proj2
import org.apache.spark._
import scala.collection.mutable.ListBuffer
import scala._

object Problem1 {
  
	// function that creates a pair and counts 1 for this pair if both words are valid
	// use list buffer so we can later work with the array
	def mapper(words:Array[String]) : ListBuffer[(String, Int)] = {
		val wordPair = new ListBuffer[(String, Int)]
		for (i <- 0 to words.length - 1) {
			val wordPair1 = words(i)
			for (j <- 0 to words.length - 1) {
			 	val wordPair2 = words(j)
			 	if (i != j) {
				 	if ((wordPair1.charAt(0) >= 'a' && wordPair2.charAt(0) >= 'a') && (wordPair1.charAt(0) <= 'z' && wordPair2.charAt(0) <= 'z')) {
						wordPair.append((wordPair1 + "," + wordPair2, 1))
				 	}
			 	}
			}
		}    
		return wordPair
	}
  
	def main(args: Array[String]) {
		// assign arguements
		val k = args(0)
		val inputFile = args(2)
		val outputFolder = args(3)
		val stopWords = args(1)
		
		//initialise spark
		val conf = new SparkConf().setAppName("Problem1").setMaster("local")
		val sc = new SparkContext(conf)
		
		//ensure k is an int
		val kValue = k.toInt
		
		//read input file and map to lower case and do same for stop words and collect into list
		val input = sc.textFile(inputFile).map(_.toLowerCase())
		val stopInput = sc.textFile(stopWords).map(_.toLowerCase())
		// put stop words into a list so we can later remove stop words from input 
		val stop: List[(String)] = stopInput.collect().toList
		
		// split at numbers and comma and pass into mapper function then reduce 
		val words = input.flatMap(line => line.split("[0-9]+,"))
		val word = words.map(line => line.split(" "))
		val wordPairs = word.flatMap(line => mapper(line)).reduceByKey(_+_)
		
		// group by splits words by their position to the comma and then takes the count value and groupsby -> adds other counts of same word sequence
		val wordPairs2 = wordPairs.map{case(x, y) => (x.split(",")(0), (x.split(",")(1), y))}.groupByKey()   //not sure if works properly
		// first word in line followed by list of all words after in the line 
		val wordPairs3 = wordPairs2.map(x => (x._1, x._2.toList))
		//convert to list				
		val list: List[(String, List[(String, Int)])] = wordPairs3.collect().toList

		var sortedList  = ListBuffer[(String, String, Int)]()
		// sort list and dont add stop words
		for(i <- 0 to list.length -1) {
      			val pair1 = list(i)
			for(j <- 0 to pair1._2.length -1) {
				val pair2 = pair1._2(j)
			 	if (!(stop.contains(pair1._1))) {
			 		if (!(stop.contains(pair2._1))) {
						sortedList.append((pair1._1, pair2._1, pair2._2))
					}
		 		}
			} 
	    	}   

	    	//change order
		val sortedList2 = sortedList.sortBy(i => i._2)
		val sortedList3 = sortedList2.sortBy(i => i._1)
		val finalSortedList = sortedList3.sortBy(i => i._3)(Ordering[Int].reverse).toList
    			
    		var finalRDDList  = ListBuffer[(String)]()
    		//print in format
		for(i <- 0 to kValue*2 - 1) {
			if (finalSortedList(i)._1 < finalSortedList(i)._2) {
				finalRDDList.append(finalSortedList(i)._1 + "," + finalSortedList(i)._2 + "\t" + finalSortedList(i)._3)
			}
		}
		
		//Convert list to RDD
		val finalRDD = sc.parallelize(finalRDDList)
		finalRDD.saveAsTextFile(outputFolder)

	}
}
