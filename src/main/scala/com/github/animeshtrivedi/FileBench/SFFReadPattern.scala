package com.github.animeshtrivedi.FileBench

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
  * Created by atr on 16.11.17.
  */
class SFFReadPattern (inputDir:String, parallel:Int){

  class Test1 extends AbstractTest {
    private var reader:HdfsByteBufferReader = _
    private val byteArray = new Array[Byte](256)

    override def init(fileName: String, expectedBytes:Long): Unit = {
      this.readBytes = expectedBytes
      fileSystem.getFileStatus(new Path(fileName))
      val px = new Path(fileName)
      this.reader = new HdfsByteBufferReader(fileSystem.open(px),
        fileSystem.getFileStatus(px))
    }

    def readFullByteArray(stream:HdfsByteBufferReader, buf:Array[Byte], toRead:Int):Unit = {
      var soFar:Int = 0
      while( soFar < toRead){
        val rx = stream.read(buf, soFar, toRead - soFar)
        if(rx == -1){
          throw new IOException(" Unexpected EOF toRead: " + toRead)
        }
        soFar+=rx
      }
    }

    override def run(): Unit = {
      val s1 = System.nanoTime()
      var nextInt = this.reader.readInt()
      while(nextInt > 0){
        readFullByteArray(this.reader, byteArray, nextInt)
        totalRows+=1
        nextInt = this.reader.readInt()
      }
      this.runTimeInNanoSecs = System.nanoTime() - s1
    }
  }


  class TestFastIterator extends AbstractTest {
    private var reader:HdfsByteBufferReader = _

    override def init(fileName: String, expectedBytes:Long): Unit = {
      this.readBytes = expectedBytes
      fileSystem.getFileStatus(new Path(fileName))
      val px = new Path(fileName)
      reader = new HdfsByteBufferReader(fileSystem.open(px),
        fileSystem.getFileStatus(px))
    }

    override def run(): Unit = {
      val itr = new FastIteratorRow(reader)
      val s1 = System.nanoTime()
      while(itr.hasNext){
        itr.next()
        totalRows+=1
      }
      this.runTimeInNanoSecs = System.nanoTime() - s1
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
  private val testArr = new Array[TestFastIterator](parallel)

  for (i <- 0 until parallel) {
    testArr(i) = new TestFastIterator()
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
  testArr.foreach(x => totalRows+=x.getResults.rows)
  var totalBytes = 0L
  filesToTest.foreach( x=>  totalBytes+=x._2)
  println("Runtime is " + (end - start)/1000000 + " msec, rows " + totalRows + " bw: " + (totalBytes * 8)/(end - start) + " Gbps")
  val runtTime = testArr.map(x => x.getResults.runtimeNanoSec).sorted
  runtTime.foreach( x => println("runtime: " + (x / 1000000) + " msec"))
}