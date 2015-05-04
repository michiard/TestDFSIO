/**
 * Created by Pietro Michiardi
 */
package fr.eurecom.dsg.spark

import java.io.{BufferedWriter, FileWriter}
import java.lang.System.{currentTimeMillis => _time}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object TestDFSIO {

  // Method to profile a code block
  def profile[R](code: => R, t: Long = _time) = (code, _time - t)

  def main(args: Array[String]) {
    // TODO: Need to display usage information, and explain arguments
    // Create a new Context
    val sc = new SparkContext(new SparkConf().setAppName("Spark DFSIO").set("spark.hadoop.dfs.replication", "1"))

    // Read or write mode
    val mode = args(0)

    // Filename to be used to write/read data to/from the storage layer
    val ioFile = args(3)

    // Get number of files and individual size
    val nFiles = args(1).toInt // corresponds to partitions
    val fSize  = args(2).toInt // in Bytes

    // Create broadcast variables that will be used later on
    val fSizeBV: Broadcast[Int] = sc.broadcast(fSize)

    // This is the output file for statistics
    val statFile = new BufferedWriter(new FileWriter("TestDFSIO_"+ mode +".stat"))

    //////////////////////////////////////////////////////////////////////
    // Write mode
    //////////////////////////////////////////////////////////////////////
    if (mode == "write") {
      // Create a Range and parallelize it, on nFiles partitions
      // The idea is to have a small RDD partitioned on a given number of workers
      // then each worker will generate data to write
    	val a = sc.parallelize(1 until nFiles+1, nFiles)

      val b = a.map( i => {
        // generate an array of Byte (8 bit), with dimension fSize
        // fill it up with "0" chars, and make it a string for it to be saved as text
        // TODO: this approach can still cause memory problems in the executor if the array is too big.
        val x = Array.ofDim[Byte](fSizeBV.value).map(x => "0").mkString(" ")
        x
      })

      // Force computation on the RDD
      sc.runJob(b, (iter: Iterator[_]) => {})

      // Write output file
      val (junk, timeW) = profile {b.saveAsTextFile(ioFile)}

      // Write statistics
      statFile.write("\nTotal volume         : " + (nFiles.toLong * fSize) + " Bytes")
      statFile.write("\nTotal write time     : " + (timeW/1000.toFloat) + " s")
      statFile.write("\nAggregate Throughput : " + (nFiles * fSize.toLong)/(timeW/1000.toFloat) + " Bytes per second")
      statFile.write("\n")
    }

    //////////////////////////////////////////////////////////////////////
    // Read mode
    //////////////////////////////////////////////////////////////////////
	if (mode == "read") {
    	// Load file(s)
    	val b = sc.textFile(ioFile,nFiles)
    	val (c, timeR) = profile {b.map(x => "0").take(1)}

    	// Write stats
    	statFile.write("\nTotal volume      : " + (nFiles * fSize.toLong) + " Bytes")
    	statFile.write("\nTotal read time   : " + (timeR/1000.toFloat) + " s")
      statFile.write("\nAggregate Throughput : " + (nFiles * fSize.toLong)/(timeR/1000.toFloat) + " Bytes per second")
    	statFile.write("\n")
	}

   	// Close open stat file
   	statFile.close()

  }
}
