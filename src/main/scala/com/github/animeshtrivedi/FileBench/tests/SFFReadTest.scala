package com.github.animeshtrivedi.FileBench.tests

import java.io.IOException
import java.nio.ByteBuffer

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.simplefileformat.SimpleFileFormat
import org.apache.spark.sql.types._

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
  private[this] var schema:StructType = _
  private[this] var colIndex:Array[(StructField, Int)] = _
  private[this] val bb = ByteBuffer.allocate(java.lang.Double.BYTES)

  private[this] var _sum:Long = 0L
  private[this] var _validDecimal:Long = 0L

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.totalBytesExpected = expectedBytes
    this.totalBytesRead = expectedBytes
    this.itr = sff.buildRowIterator(fileName)
    this.schema = sff.getSchemaFromDatafile(fileName)
    this.colIndex = this.schema.fields.zipWithIndex
  }

  final override def getResults(): TestResult = TestResult(totalRows, totalBytesRead, end - start)

  private[this] def _readInt(row:UnsafeRow, index:Int):Unit= {
    if (!row.isNullAt(index))
      this._sum += row.getInt(index)
  }

  private[this] def _readLong(row:UnsafeRow, index:Int):Unit= {
    if (!row.isNullAt(index))
      this._sum += row.getLong(index)
  }

  private[this] def _readDecimal(row:UnsafeRow, index:Int, d:DecimalType):Unit= {
    if (!row.isNullAt(index)){
      this._validDecimal+=1
      this._sum+={
        bb.putLong(0, row.getLong(index))
        bb.getDouble(0).toLong
      }
      //this._sum+=BigDecimal(row.getLong(index), d.scale).toDouble.toLong
    }
  }

  private[this] def consumeUnsafeRow(row:UnsafeRow):Unit= {
    this.colIndex.foreach( f => f._1.dataType match {
      case IntegerType => _readInt(row, f._2)
      case LongType => _readLong(row, f._2)
      case d:DecimalType => _readDecimal(row, f._2, d)
      case _ => throw new Exception(" not implemented type ")
    })
  }

  final override def run(): Unit = {
    /* here we need to consume the iterator */
    start = System.nanoTime()
    while(itr.hasNext){
      consumeUnsafeRow(itr.next().asInstanceOf[UnsafeRow])
      totalRows+=1
    }
    end = System.nanoTime()
    println(this._sum + " valid decimal " + this._validDecimal)
  }
}

object SFFReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFReadTest
}
