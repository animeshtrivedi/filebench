package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.simplefileformat.SimpleFileFormat
import org.apache.spark.sql.types._

/**
  * Created by atr on 19.11.17.
  */
class SFFReadTest extends  AbstractTest {
  private[this] val sff = new SimpleFileFormat
  private[this] var itr:Iterator[InternalRow] = _
  private[this] var schema:StructType = _
  private[this] var colIndex:Array[(StructField, Int)] = _

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.readBytes = expectedBytes
    this.itr = sff.buildRowIterator(fileName)
    this.schema = sff.getSchemaFromDatafile(fileName)
    this.colIndex = this.schema.fields.zipWithIndex
  }

  private[this] def _readInt(row:UnsafeRow, index:Int):Unit= {
    if (!row.isNullAt(index)) {
      this._sum += row.getInt(index)
      this._validInt += 1
    }
  }

  private[this] def _readLong(row:UnsafeRow, index:Int):Unit= {
    if (!row.isNullAt(index)) {
      this._sum += row.getLong(index)
      this._validLong+=1
    }
  }

  private[this] def _readDouble(row:UnsafeRow, index:Int):Unit= {
    if (!row.isNullAt(index)){
      this._sum+=row.getDouble(index).toLong
      this._validDouble+=1
    }
  }

  private[this] def _readDecimal(row:UnsafeRow, index:Int, d:DecimalType):Unit= {
    if (!row.isNullAt(index)){
      this._validDecimal+=1
      this._sum+=BigDecimal(row.getLong(index), d.scale).toDouble.toLong
    }
  }

  private[this] def consumeUnsafeRow(row:UnsafeRow):Unit= {
    this.colIndex.foreach( f => f._1.dataType match {
      case IntegerType => _readInt(row, f._2)
      case LongType => _readLong(row, f._2)
      case DoubleType => _readDouble(row, f._2)
      case _ => throw new Exception(" not implemented type ")
    })
  }

  final override def run(): Unit = {
    /* here we need to consume the iterator */
    val s1 = System.nanoTime()
    while(itr.hasNext){
      consumeUnsafeRow(itr.next().asInstanceOf[UnsafeRow])
      //itr.next().asInstanceOf[UnsafeRow]
      totalRows+=1
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    printStats()
  }
}

object SFFReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFReadTest
}
