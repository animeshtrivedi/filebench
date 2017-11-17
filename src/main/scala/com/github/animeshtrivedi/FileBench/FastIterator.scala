package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 16.11.17.
  */

class FastIterator(stream:HdfsByteBufferReader) extends Iterator[Array[Byte]] {
  private var done = false
  private var incomingSize = 0
  private var buffer: Array[Byte] = new Array[Byte](256)

  def readFullByteArray(stream:HdfsByteBufferReader, buf:Array[Byte], toRead:Int):Unit = {
    var soFar:Int = 0
    //val remainingBytesInFile = stream.capacity() - stream.position()
    //require(toRead <= remainingBytesInFile,
    //" expecting to read more " + toRead + " than in the file " + remainingBytesInFile + " bytes. File " + stream.fileName())
    while( soFar < toRead){
      val rx = stream.read(buf, soFar, toRead - soFar)
      if(rx == -1){
        //throw new IOException(" Unexpected EOF toRead: " + toRead +
        //" soFar: " + soFar +
        //" stream's position " + stream.position() +
        //" | file name " + stream.fileName() +
        //" size " + stream.capacity())
      }
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
      //unsafeRow.pointTo(buffer, incomingSize)
    }
  }

  override def hasNext():Boolean= {
    readNext()
    !done
  }
  override def next():Array[Byte] = {
    buffer
  }
}
