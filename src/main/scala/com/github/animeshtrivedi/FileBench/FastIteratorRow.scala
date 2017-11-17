package com.github.animeshtrivedi.FileBench

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
/**
  * Created by atr on 16.11.17.
  */
class FastIteratorRow(stream:HdfsByteBufferReader) extends Iterator[InternalRow] {
  private val unsafeRow = new UnsafeRow(23)
  private var done = false
  private var incomingSize = 0
  private var buffer: Array[Byte] = new Array[Byte](256)

  def readFullByteArray(stream:HdfsByteBufferReader, buf:Array[Byte], toRead:Int):Unit = {
    var soFar:Int = 0
    while( soFar < toRead){
      val rx = stream.read(buf, soFar, toRead - soFar)
      soFar+=rx
    }
  }

  def readNext(): Unit = {
    incomingSize = stream.readInt()
    if(incomingSize == -1){
      done = true
      this.stream.close()
    } else {
      if (buffer.length < incomingSize) {
        /* we resize the buffer */
        buffer = new Array[Byte](incomingSize)
      }
      /* then we read the next value */
      readFullByteArray(stream, buffer, incomingSize)
      unsafeRow.pointTo(buffer, incomingSize)
    }
  }

  override def hasNext():Boolean= {
    readNext()
    !done
  }
  override def next():InternalRow  = {
    unsafeRow
  }
}