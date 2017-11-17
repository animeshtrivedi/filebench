package com.github.animeshtrivedi.FileBench

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 17.11.17.
  */
class TestFrameWork(val allocateTestObject: AllocateTestObject, val inputDir:String, val parallel:Int) {

  private[this] val path = new Path(inputDir)
  private[this] val conf = new Configuration()
  private[this] val fileSystem = path.getFileSystem(conf)
  private[this] val allFilesEnumerated:List[(String, Long)] = Utils.enumerateWithSize(inputDir)
  println("Enumerated: " + allFilesEnumerated + " going to take " + parallel)
  private[this]val filesToTest = allFilesEnumerated.take(parallel)
  /* now we need parallel Thread objects */
  private[this] val threadArr = new Array[Thread](parallel)
  private[this] val testArr = new Array[AbstractTest](parallel)

  for (i <- 0 until parallel) {
    testArr(i) = allocateTestObject.allocate()
    threadArr(i) = new Thread(testArr(i))
  }
  filesToTest.zip(testArr).foreach( fx => {
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
  val runtTime = testArr.map(x => x.getResults()._3).sorted
  runtTime.foreach( x => println("runtime: " + (x / 1000000) + " msec"))
}
