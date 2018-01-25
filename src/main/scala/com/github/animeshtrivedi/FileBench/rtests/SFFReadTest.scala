package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.spark.sql.simplefileformat.SimpleFileFormat
import org.apache.spark.sql.simplefileformat.{SFFRowIterator, SimpleFileFormat}
import org.apache.spark.sql.simplefileformat.priv.SFFROW
import org.apache.spark.sql.types._

/**
  * Created by atr on 19.11.17.
  */
class SFFReadTest extends  AbstractTest {
  private[this] val sff = new SimpleFileFormat
  private[this] var itr:SFFRowIterator = _
  private[this] var schema:StructType = _
  private[this] var fastReaders:Array[SFFROW => Unit] = _
  private[this] var schemaArray:Array[DataType] = _
  private[this] var numCols:Int = _

  private[this] val extractLongFunc:(Int, SFFROW)=>Unit = (ordinal:Int, row:SFFROW) => {
    if(!row.isNullAt(ordinal)){
      this._sum+=row.getLong(ordinal)
      this._validLong+=1
    }
  }

  private[this] val extractIntFunc:(Int, SFFROW)=>Unit = (ordinal:Int, row:SFFROW) => {
    if(!row.isNullAt(ordinal)){
      this._sum+=row.getInt(ordinal)
      this._validInt+=1
    }
  }

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.bytesOnFS = expectedBytes
    this.itr = sff.buildRowIteratorX(fileName)
    this.schema = sff.getSchemaFromDatafile(fileName)
    this.schemaArray = this.schema.fields.map(fx => fx.dataType)
    this.numCols = this.schema.fields.length
    //    this.fastReaders = new Array[SFFROW => Unit](this.schema.size)
    //    for(i <- this.fastReaders.indices){
    //      this.schema.fields(i).dataType match {
    //        case LongType => this.fastReaders(i) = extractLongFunc(i, _:SFFROW)
    //        case IntegerType => this.fastReaders(i) = extractIntFunc(i, _:SFFROW)
    //        case _ => throw new Exception
    //      }
    //    }
  }

  private[this] def consumeSFFROWX2(row:SFFROW):Unit= {
    // TODO: to see if scala.Function are inlined or not?
    var i = 0
    while (i < numCols){
      this.fastReaders(i)(row)
      i+=1
    }
  }

  private[this] def consumeSFFROWX4Debug(row:SFFROW):Unit= {
    val sb = new StringBuilder
    var i = 0
    while (i < numCols){
      this.schemaArray(i) match {
        case IntegerType | DateType => if(!row.isNullAt(i)) {
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
        case BinaryType | StringType => if (!row.isNullAt(i)) {
          this._validBinary += 1
          val size = row.getColumnSizeinBytes(i)
          this._sum += size
          this._validBinarySize+=size
          sb.append("Binary(" + size + " bytes)")
        } else {
          sb.append("Binary(" + null + " bytes)")
        }
        case _ => throw new Exception(" not implemented type " + this.schemaArray(i))
      }
      i+=1
      sb.append(" | ")
    }
    println(sb.mkString)
  }

  private[this] def consumeSFFROWX4(row:SFFROW):Unit= {
    var i = 0
    while (i < numCols) {
      this.schemaArray(i) match {
        case IntegerType | DateType => if (!row.isNullAt(i)) {
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
        case BinaryType | StringType => if (!row.isNullAt(i)) {
          this._validBinary += 1
          val size = row.getColumnSizeinBytes(i)
          this._sum+=size
          this._validBinarySize+=size
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
      //consumeSFFROWX4Debug(row)
      consumeSFFROWX4(row)
      totalRows+=1
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    printStats()
  }
}

object SFFReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new SFFReadTest
}
