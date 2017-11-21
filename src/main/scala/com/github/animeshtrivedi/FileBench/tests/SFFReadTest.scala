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
    this._sum+= {if(row.isNullAt(11)) 0 else BigDecimal(row.getLong(11), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(12)) 0 else BigDecimal(row.getLong(12), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(13)) 0 else BigDecimal(row.getLong(13), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(14)) 0 else BigDecimal(row.getLong(14), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(15)) 0 else BigDecimal(row.getLong(15), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(16)) 0 else BigDecimal(row.getLong(16), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(17)) 0 else BigDecimal(row.getLong(17), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(18)) 0 else BigDecimal(row.getLong(18), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(19)) 0 else BigDecimal(row.getLong(19), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(20)) 0 else BigDecimal(row.getLong(20), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(21)) 0 else BigDecimal(row.getLong(21), 2) .doubleValue().toLong}
    this._sum+= {if(row.isNullAt(22)) 0 else BigDecimal(row.getLong(22), 2) .doubleValue().toLong}
  }

  private[this] def consumeUnsafeRow(row:UnsafeRow):Unit= {
    consumeUnsafeRowInt(row)
    consumeUnsafeRowDouble(row)
  }

  private[this] def consumeUnsafeRow2(row:UnsafeRow):Unit= {
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

    this._sum+= {if(row.isNullAt(11)) 0 else row.getDecimal(11 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(12)) 0 else row.getDecimal(12 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(13)) 0 else row.getDecimal(13 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(14)) 0 else row.getDecimal(14 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(15)) 0 else row.getDecimal(15 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(16)) 0 else row.getDecimal(16 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(17)) 0 else row.getDecimal(17 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(18)) 0 else row.getDecimal(18 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(19)) 0 else row.getDecimal(19 ,7, 2).toUnscaledLong}

    this._sum+= {if(row.isNullAt(20)) 0 else row.getDecimal(20 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(21)) 0 else row.getDecimal(21 ,7, 2).toUnscaledLong}
    this._sum+= {if(row.isNullAt(22)) 0 else row.getDecimal(22 ,7, 2).toUnscaledLong}
  }

  private[this] def consumeUnsafeRow3(row:UnsafeRow):Unit= {
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
    this._sum+= {if(row.isNullAt(11)) 0 else row.getDouble(11).toLong}
    this._sum+= {if(row.isNullAt(12)) 0 else row.getDouble(12).toLong}
    this._sum+= {if(row.isNullAt(13)) 0 else row.getDouble(13).toLong}
    this._sum+= {if(row.isNullAt(14)) 0 else row.getDouble(14).toLong}
    this._sum+= {if(row.isNullAt(15)) 0 else row.getDouble(15).toLong}
    this._sum+= {if(row.isNullAt(16)) 0 else row.getDouble(16).toLong}
    this._sum+= {if(row.isNullAt(17)) 0 else row.getDouble(17).toLong}
    this._sum+= {if(row.isNullAt(18)) 0 else row.getDouble(18).toLong}
    this._sum+= {if(row.isNullAt(19)) 0 else row.getDouble(19).toLong}

    this._sum+= {if(row.isNullAt(20)) 0 else row.getDouble(20).toLong}
    this._sum+= {if(row.isNullAt(21)) 0 else row.getDouble(21).toLong}
    this._sum+= {if(row.isNullAt(22)) 0 else row.getDouble(22).toLong}
  }

  final override def run(): Unit = {
    /* here we need to consume the iterator */
    start = System.nanoTime()
    while(itr.hasNext){
      consumeUnsafeRow(itr.next().asInstanceOf[UnsafeRow])
      totalRows+=1
    }
    end = System.nanoTime()
    println(this._sum)
  }
}

object SFFReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFReadTest
}
