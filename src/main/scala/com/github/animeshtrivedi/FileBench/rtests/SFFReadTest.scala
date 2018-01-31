package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, JavaUtils, TestObjectFactory}
import org.apache.spark.sql.simplefileformat.SimpleFileFormat
import org.apache.spark.sql.simplefileformat.filters.{LessThanEqualFilter, SFFFilter}
import org.apache.spark.sql.simplefileformat.{SFFRowIterator, SimpleFileFormat}
import org.apache.spark.sql.simplefileformat.priv.SFFROW
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by atr on 19.11.17.
  */
class SFFReadTest extends  AbstractTest {
  private[this] val sff = new SimpleFileFormat
  private[this] var itr:SFFRowIterator = _
  private[this] var schema:StructType = _
  private[this] var fastReadersV1:Array[SFFROW => Unit] = _
  private[this] var fastReadersV2:Array[(Int, SFFROW) => Unit] = _
  private[this] var schemaArray:Array[DataType] = _
  private[this] var numCols:Int = _

  private[this] val extractDoubleFunc:(Int, SFFROW)=>Unit = (ordinal:Int, row:SFFROW) => {
    if (!row.isNullAt(ordinal)) {
      this._validDouble += 1
      this._sum += row.getDouble(ordinal).toLong
    }
  }

  private[this] val extractLongFunc:(Int, SFFROW)=>Unit = (ordinal:Int, row:SFFROW) => {
    if(!row.isNullAt(ordinal)){
      this._sum+=row.getLong(ordinal)
      this._validLong+=1
    }
  }

  //probably create an array of these functions too
  private[this] val extractIntFunc:(Int, SFFROW)=>Unit = (ordinal:Int, row:SFFROW) => {
    if(!row.isNullAt(ordinal)){
      this._sum+=row.getInt(ordinal)
      this._validInt+=1
    }
  }

  private[this] def setupV1():Unit = {
    this.fastReadersV1 = new Array[SFFROW => Unit](numCols)
    var i = 0
    println(" Fast function read will have : " + numCols + " functions ")
    while(i < numCols) {
      this.schema.fields(i).dataType match {

        case LongType => this.fastReadersV1(i) = {
          (row: SFFROW) => {
            val index = i
            if (!row.isNullAt(index)) {
              this._sum += row.getLong(index)
              this._validLong += 1
            }
          }
        }

        case DoubleType => this.fastReadersV1(i) = {
          (row:SFFROW) => {
            val index = i
            if(!row.isNullAt(index)){
              this._sum+=row.getDouble(index).toLong
              this._validDouble+=1
            }
          }
        }

        case IntegerType => this.fastReadersV1(i) = {
          (row:SFFROW) => {
            val index = i
            if(!row.isNullAt(index)){
              this._sum+=row.getInt(index)
              this._validInt+=1
            }
          }
        }
        case _ => throw new Exception
      }
      i+=1
    }
  }

  private[this] def setupV2():Unit = {
    this.fastReadersV2 = new Array[(Int, SFFROW) => Unit](numCols)
    var i = 0
    println(" V2 Fast function read will have : " + numCols + " functions ")
    while(i < numCols) {
      this.schema.fields(i).dataType match {
        case LongType => this.fastReadersV2(i) = extractLongFunc
        case DoubleType => this.fastReadersV2(i) = extractDoubleFunc
        case IntegerType => this.fastReadersV2(i) = extractIntFunc
        case _ => throw new Exception
      }
      i+=1
    }
  }

  private[this] final def makeProjectionSchema(dataSchema:StructType):StructType = {
    if(JavaUtils.projection == 100 || !JavaUtils.enableProjection){
      println("Projected schema is 100% (or no projection enabled) ")
      dataSchema
    } else {
      println("Projected schema is " + JavaUtils.projection + "%")
      val numCols = dataSchema.fields.length
      val choosen = (numCols * JavaUtils.projection) / 100
      val choosenCols = dataSchema.fields.take(choosen)
      var newSchema = new StructType()
      choosenCols.foreach(fx => newSchema = newSchema.add(fx))
      newSchema
    }
  }

  private[this] final def makeFilter():ArrayBuffer[SFFFilter] = {
    if(JavaUtils.enableSelection) {
      ArrayBuffer(new LessThanEqualFilter(0, IntegerType, JavaUtils.selection))
    } else {
      ArrayBuffer[SFFFilter]()
    }
  }

  private[this] final def initProjectionFilters(fileName: String, expectedBytes: Long): Unit = {
    this.bytesOnFS = expectedBytes
    val originalSchema = sff.getSchemaFromDatafile(fileName)
    // make filters
    val filters = makeFilter()
    // this is projected schema
    this.schema = makeProjectionSchema(originalSchema)
    this.itr = sff.makeRowIteratorZ(fileName, originalSchema, this.schema, filters)
    this.schemaArray = this.schema.fields.map(fx => fx.dataType)
    this.numCols = this.schema.fields.length
  }

  private[this] final def initFast(fileName: String, expectedBytes: Long): Unit = {
    this.bytesOnFS = expectedBytes
    this.itr = sff.buildRowIteratorX(fileName)
    this.schema = sff.getSchemaFromDatafile(fileName)
    this.schemaArray = this.schema.fields.map(fx => fx.dataType)
    this.numCols = this.schema.fields.length
  }

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    //initFast(fileName, expectedBytes)
    initProjectionFilters(fileName, expectedBytes)
    //setupV1()
    //setupV2()
  }

  private[this] def consumeSFFROWX1(row:SFFROW):Unit= {
    // TODO: to see if scala.Function are inlined or not?
    // this does not work because function pointers do not take variable indexes
    var i = 0
    while (i < numCols){
      println(" i " + i + " numCols: " + numCols)
      this.fastReadersV1(i).apply(row)
      i+=1
    }
  }

  private[this] def consumeSFFROWX2(row:SFFROW):Unit= {
    var i = 0
    while (i < numCols){
      this.fastReadersV2(i)(i, row)
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
