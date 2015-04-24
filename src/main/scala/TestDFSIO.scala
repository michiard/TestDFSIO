package fr.eurecom.dsg.spark

import java.io._
import java.lang.System.{currentTimeMillis => _time}

import org.apache.spark.{SparkConf, SparkContext}

object TestDFSIO {

  // Method to profile a code block
  def profile[R](code: => R, t: Long = _time) = (code, _time - t)

  def main(args: Array[String]) {
    // Create a new Context
    val sc = new SparkContext(new SparkConf().setAppName("Spark DFSIO"))

    // Read or write mode
    val mode = args(0)
    // Filename to be used to write/read data to/from the storage layer
    val ioFile = args(3)

    // Get number of files and individual size
    val nFiles = args(1).toInt
    val fSize  = args(2).toInt // must be multiple of 2 bytes

    // This is the output file for statistics
    val statFile = new BufferedWriter(new FileWriter("TestDFSIO_"+ mode +".stat"))

    //////////////////////////////////////////////////////////////////////
    // Write mode
    //////////////////////////////////////////////////////////////////////
    if (mode == "write") {
    	// Generate a RDD full strings of "1" character (2 bytes)
    	// E.g.: 4 files of 200 bytes each => 4 * 200 / 2 = 400
    	val tmp = Array.ofDim[String](nFiles * fSize / 2).map(x => "1")
    	val a = sc.parallelize(tmp,nFiles)
    	
    	// Write output file
    	// This is a text file
    	val (junk, timeW) = profile {a.saveAsTextFile(ioFile)}
    	statFile.write("\nTotal volume       : " + (nFiles * fSize / 2) + "bytes")
    	statFile.write("\nTotal write time   : " + (timeW/1000) + "s")
    	statFile.write("\n")
	}

    //////////////////////////////////////////////////////////////////////
    // Read mode
    //////////////////////////////////////////////////////////////////////
	if (mode == "read") {
    	// Load file(s)
    	val b = sc.textFile(ioFile,nFiles)
    	val (c, timeR) = profile {b.map(x => "0").max}

    	// Write stats
    	statFile.write("\nTotal volume      : " + (nFiles * fSize / 2) + "bytes")
    	statFile.write("\nTotal read time   : " + (timeR/1000) + "s")
    	statFile.write("\n")
	}

   	// Close open stat file
   	statFile.close()

  }
}

