package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.simplefileformat.SimpleFileFormat
import org.apache.spark.sql.simplefileformat.priv.SFFRow
import org.apache.spark.sql.types._

/**
  * Created by atr on 19.11.17.
  */
class SFFReadTest extends  AbstractTest {
  private[this] val sff = new SimpleFileFormat
  private[this] var itr:Iterator[SFFRow] = _
  private[this] var schema:StructType = _
  private[this] var fastReaders:Array[SFFRow => Unit] = _
  private[this] var schemaArray:Array[DataType] = _
  private[this] var numCols:Int = _

  private[this] val extractLongFunc:(Int, SFFRow)=>Unit = (ordinal:Int, row:SFFRow) => {
    if(!row.isNullAt(ordinal)){
      this._sum+=row.getLong(ordinal)
      this._validLong+=1
    }
  }

  private[this] val extractIntFunc:(Int, SFFRow)=>Unit = (ordinal:Int, row:SFFRow) => {
    if(!row.isNullAt(ordinal)){
      this._sum+=row.getInt(ordinal)
      this._validInt+=1
    }
  }

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.readBytes = expectedBytes
    this.itr = sff.buildRowIteratorX(fileName)
    this.schema = sff.getSchemaFromDatafile(fileName)
    this.schemaArray = this.schema.fields.map(fx => fx.dataType)
    this.numCols = this.schema.fields.length
    //    this.fastReaders = new Array[SFFRow => Unit](this.schema.size)
    //    for(i <- this.fastReaders.indices){
    //      this.schema.fields(i).dataType match {
    //        case LongType => this.fastReaders(i) = extractLongFunc(i, _:SFFRow)
    //        case IntegerType => this.fastReaders(i) = extractIntFunc(i, _:SFFRow)
    //        case _ => throw new Exception
    //      }
    //    }
  }

  private[this] def consumeSFFRowX2(row:SFFRow):Unit= {
    // TODO: to see if scala.Function are inlined or not?
    for(i <- 0 until numCols){
      this.fastReaders(i)(row)
    }
  }

  private[this] def consumeSFFRowX4(row:SFFRow):Unit= {
    for(i <- 0 until numCols){
      this.schemaArray(i) match {
        case IntegerType => if(!row.isNullAt(i)) {
          this._validInt += 1
          this._sum += row.getInt(i)
        }
        case LongType => if(!row.isNullAt(i)) {
          this._validLong += 1
          this._sum += row.getLong(i)
        }
        case _ => throw new Exception(" not implemented type ")
      }
    }
  }

  final override def run(): Unit = {
    /* here we need to consume the iterator */
    val s1 = System.nanoTime()
    while(itr.hasNext){
      val row = itr.next()
      consumeSFFRowX4(row)
      totalRows+=1
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    printStats()
  }
}

object SFFReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFReadTest
}
