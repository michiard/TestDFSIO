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
    // Create a new Context
    val sc = new SparkContext(new SparkConf().setAppName("Spark DFSIO"))

    // Read or write mode
    val mode = args(0)
    val ioFile = args(3)

    // Get number of files and individual size
    val nFiles = args(1).toInt
    val fSize  = args(2).toInt

    // This is the output file for statistics
    val statFile = new BufferedWriter(new FileWriter("TestDFSIO_"+ mode +".stat"))

    //////////////////////////////////////////////////////////////////////
    // Write mode
    //////////////////////////////////////////////////////////////////////
    if (mode == "write") {
    	// Generate a RDD full of numbers
    	val a = sc.parallelize(1 to (fSize*nFiles), nFiles).map(x => x.toLong) // this is 115MB
    	// fSize = 1e7 ~ 115 MB for one writer
    	// nFiles = actually takes fSize and equally divides it for nFiles
    	// This is why we have fSize*nFiles as the file size

    	// Write output file
		// This is going to be saved as a binary object file
    	val (junk, timeW) = profile {a.saveAsObjectFile(ioFile)}
    	statFile.write("\nTotal volume       : " + (nFiles * fSize) + " bytes")
        statFile.write("\nTotal write time   : " + (timeW/1000) + " s")
        statFile.write("\n")
	}

    //////////////////////////////////////////////////////////////////////
    // Read mode
    //////////////////////////////////////////////////////////////////////
	if (mode == "read") {
    	// Load file(s)
    	val b = sc.objectFile[Long](ioFile)
    	val (c, timeR) = profile {b.map(x => x+1).max}

        // Write stats
        statFile.write("\nTotal volume      : " + (nFiles * fSize) + " bytes")
        statFile.write("\nTotal read time   : " + (timeR/1000) + " s")
        statFile.write("\n")
	}

   	// Close open stat file
   	statFile.close()
  }
}

