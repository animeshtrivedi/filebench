package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
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

  private[this] var _sum:Long = 0L
  private[this] var _validDecimal:Long = 0L

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.totalBytesExpected = expectedBytes
    this.totalBytesRead = expectedBytes
    this.itr = sff.buildRowIterator(fileName)
  }

  final override def getResults(): TestResult = TestResult(totalRows, totalBytesRead, end - start)

  private[this] def consumeUnsafeRowInt(row:UnsafeRow):Unit= {
    this._sum+= row.getInt(0)
    this._sum+= row.getInt(1)
    this._sum+= row.getInt(2)
    this._sum+= row.getInt(3)
    this._sum+= row.getInt(4)
    this._sum+= row.getInt(5)
    this._sum+= row.getInt(6)
    this._sum+= row.getInt(7)
    this._sum+= row.getInt(8)

    this._sum+= row.getLong(9)

    this._sum+= row.getInt(10)
  }

  private[this] def consumeUnsafeRowDouble(row:UnsafeRow):Unit= {
    for(i <- 11 until 23){
      if(!row.isNullAt(i)){
        this._validDecimal+=1
        this._sum+=BigDecimal(row.getLong(i), 2).toDouble.toLong
      }
    }
  }

  private[this] def consumeUnsafeRow(row:UnsafeRow):Unit= {
    consumeUnsafeRowInt(row)
    consumeUnsafeRowDouble(row)
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
