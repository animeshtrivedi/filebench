package com.github.animeshtrivedi.FileBench

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
  * Created by atr on 17.11.17.
  */
class HdfsReadTest extends AbstractTest {
  private var instream:FSDataInputStream = _
  private val byteArr = new Array[Byte](1024 * 1024)
  private var expectedIncomingBytes = 0L


  override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    this.instream = fileSystem.open(path)
    this.expectedIncomingBytes = expectedBytes
  }

  override def getResults(): (Long, Long, Long) = (0, this.expectedIncomingBytes, 0)

  override def run(): Unit = {
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

object HdfsReadTest extends AllocateTestObject {
  final override def allocate(): AbstractTest = new HdfsReadTest
}

