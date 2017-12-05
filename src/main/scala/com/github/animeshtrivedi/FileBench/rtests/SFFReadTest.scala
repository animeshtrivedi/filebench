package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
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
    var i = 0
    while (i < numCols){
      this.fastReaders(i)(row)
      i+=1
    }
  }

  private[this] def consumeSFFRowX4Debug(row:SFFRow):Unit= {
    val sb = new StringBuilder
    var i = 0
    while (i < numCols){
      this.schemaArray(i) match {
        case IntegerType => if(!row.isNullAt(i)) {
          this._validInt += 1
          val x = row.getInt(i)
          this._sum += x
          sb.append(" " + x + " ")
        } else {
          sb.append(" null ")
        }
        case LongType => if(!row.isNullAt(i)) {
          this._validLong += 1
          val x = row.getLong(i)
          this._sum += x
          sb.append(" " + x + " ")
        }else {
          sb.append(" null ")
        }
        case DoubleType => if(!row.isNullAt(i)) {
          this._validDouble += 1
          val x = row.getDouble(i)
          this._sum += x.toLong
          sb.append(" " + x + " ")
        }else {
          sb.append(" null ")
        }
        case _ => throw new Exception(" not implemented type " + this.schemaArray(i))
      }
      i+=1
    }
    println(sb.mkString)
  }

  private[this] def consumeSFFRowX4(row:SFFRow):Unit= {
    var i = 0
    while (i < numCols) {
      this.schemaArray(i) match {
        case IntegerType => if (!row.isNullAt(i)) {
          this._validInt += 1
          this._sum += row.getInt(i)
        }
        case LongType => if (!row.isNullAt(i)) {
          this._validLong += 1
          this._sum += row.getLong(i)
        }
        case DoubleType => if (!row.isNullAt(i)) {
          this._validDouble += 1
          this._sum += row.getDouble(i).toLong
        }
        case _ => throw new Exception(" not implemented type " + this.schemaArray(i))
      }
      i+=1
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
