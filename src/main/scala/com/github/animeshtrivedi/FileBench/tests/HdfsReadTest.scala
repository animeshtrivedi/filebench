package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
  * Created by atr on 17.11.17.
  */
class HdfsReadTest extends AbstractTest {
  private[this] var instream:FSDataInputStream = _
  private[this] val byteArr = new Array[Byte](1024 * 1024)
  private[this] var expectedIncomingBytes = 0L

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    this.instream = fileSystem.open(path)
    this.expectedIncomingBytes = expectedBytes
  }

  final override def getResults():TestResult = TestResult(0, this.expectedIncomingBytes, 0)

  final override def run(): Unit = {
    var rx = instream.read(byteArr)
    var bytes:Long = 0L
    while (rx > 0) {
      bytes+=rx
      rx = instream.read(byteArr)
    }
    require(this.expectedIncomingBytes == bytes)
    println (instream + " read " + bytes + " Bytes")
  }
}

object HdfsReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new HdfsReadTest
}

