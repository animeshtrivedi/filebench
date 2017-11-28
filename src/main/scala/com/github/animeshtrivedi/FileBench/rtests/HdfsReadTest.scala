package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
  * Created by atr on 17.11.17.
  */
class HdfsReadTest extends AbstractTest {
  private[this] var instream:FSDataInputStream = _
  private[this] val byteArr = new Array[Byte](1024 * 1024)

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    this.instream = fileSystem.open(path)
    this.readBytes = expectedBytes
  }

  final override def run(): Unit = {
    val s1 = System.nanoTime()
    var rx = instream.read(byteArr)
    var bytes:Long = 0L
    while (rx > 0) {
      bytes+=rx
      rx = instream.read(byteArr)
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    require(this.readBytes == bytes)
    println (instream + " read " + bytes + " Bytes")
  }
}

object HdfsReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new HdfsReadTest
}

