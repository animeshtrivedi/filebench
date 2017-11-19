package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.simplefileformat.SimpleFileFormat

/**
  * Created by atr on 19.11.17.
  */
class SFFReadTest extends  AbstractTest {
  private[this] var totalBytesRead = 0L
  private[this] var totalBytesExpected = 0L
  private[this] var totalRows = 0L
  private[this] val sff = new SimpleFileFormat
  private[this] var itr:Iterator[InternalRow] = _
  private[this] var start = 0L
  private[this] var end = 0L

  override def init(fileName: String, expectedBytes: Long): Unit = {
    this.totalBytesExpected = expectedBytes
    this.totalBytesRead = expectedBytes
    this.itr = sff.buildRowIterator(fileName)
  }

  override def getResults(): TestResult = TestResult(totalRows, totalBytesRead, end - start)

  override def run(): Unit = {
    /* here we need to consume the iterator */
    start = System.nanoTime()
    while(itr.hasNext){
      itr.next()
      totalRows+=1
    }
    end = System.nanoTime()
  }
}

object SFFReadTest extends TestObjectFactory {
  override def allocate(): AbstractTest = new SFFReadTest
}
