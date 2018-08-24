package com.github.animeshtrivedi.FileBench.rtests

import java.nio.ByteBuffer

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, Utils}
import org.apache.spark.sql.simplefileformat.priv.SFFROW
import org.apache.spark.sql.simplefileformat.{SFFRowIterator, SimpleFileFormat}
import org.apache.spark.sql.types._

/**
  * Created by atr on 22.08.18.
  */
class SFFCsum extends  AbstractTest {
  private[this] val sff = new SimpleFileFormat
  private[this] var itr:SFFRowIterator = _
  private[this] var schema:StructType = _
  private[this] var schemaArray:Array[DataType] = _
  private[this] var numCols:Int = _


  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.bytesOnFS = expectedBytes
    this.itr = sff.buildRowIteratorX(fileName)
    this.schema = sff.getSchemaFromDatafile(fileName)
    this.schemaArray = this.schema.fields.map(fx => fx.dataType)
    this.numCols = this.schema.fields.length
  }

  private[this] def consumeSFFROWX4(row:SFFROW):Unit= {
    var bb:ByteBuffer = ByteBuffer.allocate(java.lang.Double.BYTES)
    var i = 0
    while (i < numCols) {
      // match at the top
      if(!row.isNullAt(i)) {
        this.schemaArray(i) match {
          case IntegerType | DateType =>
            this._validInt += 1
            this._sum += row.getInt(i)

          case LongType =>
            this._validLong += 1
            this._sum += row.getLong(i)

          case DoubleType =>
            this._validDouble += 1
            bb.putDouble(0,row.getDouble(i))
            this._sum += Utils.arrayChecksum(bb.array(), 0, java.lang.Double.BYTES)

          case BinaryType | StringType =>
            this._validBinary += 1
            val size = row.getColumnSizeinBytes(i)
            if( size > bb.capacity()){
              bb = ByteBuffer.allocate(size)
            }
            val buffer = bb.array()
            row.getBinary(i, buffer, 0, size)
            /* now we calculate checksum */
            this._sum += Utils.arrayChecksum(buffer, 0, size)
            this._validBinarySize += size

          case _ => throw new Exception(" not implemented type " + this.schemaArray(i))
        }
      }
      i+=1
    }
    this.totalRows+=1
  }

  final override def run(): Unit = {
    /* here we need to consume the iterator */
    val s1 = System.nanoTime()
    while(itr.hasNext){
      val row = itr.next()
      consumeSFFROWX4(row)
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    printStats()
  }
}


object SFFCsum extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFCsum
}

