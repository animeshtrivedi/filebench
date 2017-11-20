package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.simplefileformat.SimpleFileFormat
import org.apache.spark.sql.types.Decimal

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

  private[this] var longSum:Long = 0L
  private[this] var doubleSum:Double = 0

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.totalBytesExpected = expectedBytes
    this.totalBytesRead = expectedBytes
    this.itr = sff.buildRowIterator(fileName)
  }

  final override def getResults(): TestResult = TestResult(totalRows, totalBytesRead, end - start)

  private[this] def consumeUnsafeRow(row:UnsafeRow):Unit= {
    longSum+= row.getInt(0)
    longSum+= row.getInt(1)
    longSum+= row.getInt(2)
    longSum+= row.getInt(3)
    longSum+= row.getInt(4)
    longSum+= row.getInt(5)
    longSum+= row.getInt(6)
    longSum+= row.getInt(7)
    longSum+= row.getInt(8)
    longSum+= row.getLong(9)
    longSum+= row.getInt(10)
    row.getDouble(???)
    doubleSum+= {if(row.isNullAt(11)) 0 else BigDecimal(row.getLong(11), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(12)) 0 else BigDecimal(row.getLong(12), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(13)) 0 else BigDecimal(row.getLong(13), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(14)) 0 else BigDecimal(row.getLong(14), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(15)) 0 else BigDecimal(row.getLong(15), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(16)) 0 else BigDecimal(row.getLong(16), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(17)) 0 else BigDecimal(row.getLong(17), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(18)) 0 else BigDecimal(row.getLong(18), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(19)) 0 else BigDecimal(row.getLong(19), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(20)) 0 else BigDecimal(row.getLong(20), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(21)) 0 else BigDecimal(row.getLong(21), 2).doubleValue()}
    doubleSum+= {if(row.isNullAt(22)) 0 else BigDecimal(row.getLong(22), 2).doubleValue()}
  }

  final override def run(): Unit = {
    /* here we need to consume the iterator */
    start = System.nanoTime()
    while(itr.hasNext){
      consumeUnsafeRow(itr.next().asInstanceOf[UnsafeRow])
      totalRows+=1
    }
    end = System.nanoTime()
    println(this.doubleSum + " " + this.longSum)
  }
}

object SFFReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFReadTest
}
