package com.github.animeshtrivedi.FileBench.rtests

import java.io.EOFException

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
  * Created by atr on 17.11.17.
  */
class HdfsReadTest extends AbstractTest {
  private[this] var instream:FSDataInputStream = _
  private[this] val byteArr = new Array[Byte](1048575) //(1024 * 1024)

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    this.instream = fileSystem.open(path)
    this.readBytes = expectedBytes
  }

  final override def run(): Unit = runUnaligned()

  final def runFastest(): Unit = {
    val s1 = System.nanoTime()
    var rx = instream.read(byteArr)
    var bytes:Long = 0L
    while (rx > 0) {
      bytes+=rx
      rx = instream.read(byteArr)
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    require(this.readBytes == bytes)
  }

  final def readFullByteArray(arr: Array[Byte], offset: Int, length: Int): Int = {
    /* there is no nicer way to handle this problem of unaligned reads. For example when reading the int schema
    * we have a pattern of reading 1048575 bytes, which breaks into two call of some 2-4 bytes and then the big
    * chunk. And that is what kills the performance. For reading - I hope this improves on Crail */
    var soFar:Int = 0
    while( soFar < length){
      val rx = instream.read(arr, soFar, length - soFar)
      if(rx == -1){
        throw new EOFException(" Unexpected EOF toRead: " + length +
          " soFar: " + soFar)
      }
      soFar+=rx
    }
    length
  }


  final def runUnaligned(): Unit = {
    val s1 = System.nanoTime()
    var bytesLeft = this.readBytes
    while (bytesLeft > 0 ) {
      val toRead = Math.min(bytesLeft, byteArr.length.toLong).toInt
      readFullByteArray(byteArr, 0, toRead)
      bytesLeft-=toRead
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
  }
}

object HdfsReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new HdfsReadTest
}

