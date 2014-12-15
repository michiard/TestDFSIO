package fr.eurecom.dsg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.io._
import System.{currentTimeMillis => _time}

object TestDFSIO {

  // Method to profile a code block
  def profile[R](code: => R, t: Long = _time) = (code, _time - t)

  def main(args: Array[String]) {
    // This is the output file for statistics
    val statFile = new BufferedWriter(new FileWriter("TestDFSIO.stat"))

    // Create a new Context
    val sc = new SparkContext(new SparkConf().setAppName("Spark DFSIO"))

    // Read or write mode
    val mode = args(0)
    val ioFile = args(3)

    // Get number of files and individual size
    val nFiles = args(1).toInt
    val fSize  = args(2).toInt

    if (mode == "write") {

    //////////////////////////////////////////////////////////////////////
    // Write mode //
    //////////////////////////////////////////////////////////////////////
    	// Generate a RDD full of numbers
    	val a = sc.parallelize(1 to (fSize*nFiles), nFiles).map(x => x.toLong) // this is 115MB
    	// fSize = 1e7 ~ 115 MB for one writer
    	// nFiles = actually takes fSize and equally divides it for nFiles
    	// This is why we have fSize*nFiles as the file size

    	// Write output file
    	val (junk, timeW) = profile {a.saveAsObjectFile(ioFile)}
    	statFile.write("\n\nTime for write : " + timeW/1000 + "s \n")

    	// Free up memory
    	a.unpersist()

	   	// Close open stat file
    	statFile.close()

	}

	if (mode == "read") {

    //////////////////////////////////////////////////////////////////////
    // Read mode //
    //////////////////////////////////////////////////////////////////////

    	// Load file(s)
    	val b = sc.objectFile[Long](ioFile)
    	val (c, timeR) = profile {b.map(x => x+1).max}

    	// Write stats
    	statFile.write("\n\nTime for read : " + timeR/1000 + "s \n")

    	// Free up memory
    	b.unpersist()

    	// Close open stat file
    	statFile.close()
	}
  }
}

