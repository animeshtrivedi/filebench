package com.github.animeshtrivedi.FileBench

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SimpleFileFormat
import org.apache.spark.sql.catalyst.InternalRow

/**
  * Created by atr on 14.11.17.
  */
class SFFTest(inputDir:String, parallel:Int){

  class Test extends AbstractTest {
    private var totalBytesRead = 0L
    private var totalBytesExpected = 0L
    private var totalRows = 0L
    private val sff = new SimpleFileFormat
    private var itr:Iterator[InternalRow] = _

    override def init(fileName: String, expectedBytes:Long): Unit = {
      this.totalBytesExpected = expectedBytes
      this.totalBytesRead = expectedBytes
      this.itr = sff.buildRowIterator(fileName)
    }

    override def run(): Unit = {
      /* here we need to consume the iterator */
      while(itr.hasNext){
        itr.next()
        totalRows+=1
      }
    }

    override def getResults():(Long, Long) = {
      /* here we need to run the count */
      require(totalBytesRead == totalBytesExpected)
      (totalRows, totalBytesRead)
    }
  }

  private val path = new Path("/")
  private val conf = new Configuration()
  private val fileSystem = path.getFileSystem(conf)
  private val allFilesEnumerated:List[(String, Long)] = Utils.enumerateWithSize(inputDir)
  println("Enumerated: " + allFilesEnumerated + " going to take " + parallel)
  private val filesToTest = allFilesEnumerated.take(parallel)
  /* now we need parallel Thread objects */
  private val threadArr = new Array[Thread](parallel)
  private val testArr = new Array[Test](parallel)

  for (i <- 0 until parallel) {
    testArr(i) = new Test()
    threadArr(i) = new Thread(testArr(i))
  }

  val a1 = filesToTest.zip(testArr)

  a1.foreach( fx => {
    fx._2.init(fx._1._1, fx._1._2)
  })

  /////////////////////////////////////////
  val start = System.nanoTime()
  for (i <- 0 until parallel) {
    threadArr(i).start()
  }
  for (i <- 0 until parallel) {
    threadArr(i).join()
  }
  val end = System.nanoTime()
  /////////////////////////////////////////

  var totalRows = 0L
  testArr.foreach(x => totalRows+=x.getResults()._1)
  var totalBytes = 0L
  filesToTest.foreach( x=>  totalBytes+=x._2)
  println(" Runtime is " + (end - start)/1000000 + " msec, rows " + totalRows + " bw: " + (totalBytes * 8)/(end - start) + " Gbps")
}