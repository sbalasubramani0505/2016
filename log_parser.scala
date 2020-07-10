package com.InstaCart.source

/* Simple Log Parser */

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/** Exception declarations **/
class eRaiseException() extends java.lang.Exception() {}

object InstaCartLogParser {
    
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf().setAppName("InstaCartLogParser").setMaster("local")
    val sc = new SparkContext(conf)
    val OutputDir = "/user/sbalasubramani/InstaCart/SparkOutput"  //Change OutputDir
      
    /* Execute the below 3 statements in spark shell if spark-submit is not used */
    // ************
    val InputFile = sc.textFile("scripting_challenge_input_file.txt").mapPartitions(_.drop(1)) 
    val AllColsRDD = InputFile.map(_.split("\t")).map( a => try {( a(0).split(":")(0) , a(0).split(":")(1).substring(0,4) + "-" + a(0).split(":")(1).substring(4,6)+ "-" + a(0).split(":")(1).takeRight(2),  a(1) , (a(2).toDouble + a(3).toDouble + a(4).toDouble + a(5).toDouble )/4 , a(6) , if (a(6).startsWith("http://www.insacart.com/")) { } else {"Error in URL"} )} catch {
        case e : Exception =>
        // Log error
	      "Malformed record"
      })
    AllColsRDD.coalesce(1).saveAsTextFile(OutputDir)
    // ************
    
    //Stop the Spark context
    sc.stop

  }

}
