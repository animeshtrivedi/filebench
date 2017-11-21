package com.github.animeshtrivedi.FileBench.tests

import java.math.BigInteger
import java.nio.charset.Charset

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}
import org.apache.parquet.io.{ColumnIOFactory, MessageColumnIO}
import org.apache.parquet.io.api.{GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, PrimitiveType}

/**
  * Created by atr on 19.11.17.
  */
class ParquetReadTest extends AbstractTest {

  private[this] var parquetFileReader:ParquetFileReader = _
  private[this] var expectedRows:Long = _
  private[this] var schema:MessageType = _
  private[this] var mdata:FileMetaData = _
  private[this] var runTime = 0L
  private[this] var readBytes = 0L

  private[this] var _sum:Long = 0L
  private[this] var _validDecimal:Long = 0L

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val readFooter:ParquetMetadata = ParquetFileReader.readFooter(conf,
      path,
      ParquetMetadataConverter.NO_FILTER)
    this.mdata = readFooter.getFileMetaData
    this.schema = mdata.getSchema
    this.parquetFileReader = ParquetFileReader.open(conf, path)
    this.expectedRows = parquetFileReader.getRecordCount
    this.readBytes = expectedBytes
  }

  final override def getResults(): TestResult = TestResult(expectedRows, this.readBytes, runTime)

  final override def run(): Unit = run1()

  final def run2():Unit = {
    var pages:PageReadStore = null
    val s2 = System.nanoTime()
    try
    {
      var contx = true
      while (contx) {
        pages = parquetFileReader.readNextRowGroup()
        if (pages != null) {
          val rows = pages.getRowCount
          var columnIO: MessageColumnIO = new ColumnIOFactory().getColumnIO(schema)
          val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
          for (i <- 0L until rows) {
            val encodedRow = recordReader.read().asGroup()
            // here we can convert it to raw values
          }
        } else {
          contx = false
        }
      }
    }
    catch
      {
        case foo: Exception => foo.printStackTrace()
      }
    finally {
      val s3 = System.nanoTime()
      parquetFileReader.close()
      val s4 = System.nanoTime()
      this.runTime = s3 - s2
    }
  }

  final def run1():Unit = {
    var tempRows = 0L
    var rowBatchesx = 0L
    var readSoFarRows = 0L
    var pageReadStore:PageReadStore = null
    val colDesc = schema.getColumns

    val explictType = List(
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT64,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.DOUBLE
    )

    val conv = new DumpGroupConverter
    val s2 = System.nanoTime()
    try
    {
      var contx = true
      while (contx) {
        pageReadStore = parquetFileReader.readNextRowGroup()
        if (pageReadStore != null) {
          rowBatchesx+=1
          val colReader = new ColumnReadStoreImpl(pageReadStore, conv,
            schema, mdata.getCreatedBy)
          for(i <-0 until colDesc.size()){
            tempRows += consumeColumn(colReader, colDesc.get(i), explictType(i))
          }
          readSoFarRows+=(tempRows / colDesc.size())
          tempRows = 0L
        } else {
          contx = false
        }
      }
    } catch {
      case foo: Exception => foo.printStackTrace()
    } finally {
      val s3 = System.nanoTime()
      runTime = s3 - s2
      parquetFileReader.close()
      require(readSoFarRows == expectedRows,
        " readSoFar " + readSoFarRows + " and expectedRows " + expectedRows + " do not match ")
      println(this._sum + " valid decimal " + this._validDecimal)
    }
  }

  private [this] class DumpGroupConverter extends GroupConverter {
    final def start() {}
    final def end() {}
    final def getConverter(fieldIndex: Int) = new DumpConverter
  }

  private [this] class DumpConverter extends PrimitiveConverter {
    final override def asGroupConverter = new DumpGroupConverter
  }

  private [this] def consumeColumn(crstore: ColumnReadStoreImpl, column: org.apache.parquet.column.ColumnDescriptor,
                                   matchType:PrimitiveTypeName): Long = {
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var rowsX = 0L
    for (i <- 0L until rows){
      val rvalue = creader.getCurrentRepetitionLevel
      val dvalue = creader.getCurrentDefinitionLevel
      //println(" rvalue " + rvalue + " dvalue " + dvalue)
      if(dvalue == 1){ // flat schema rvalue is zero and dvalue is 0 or 1
        rowsX+=1
        val value = column.getType match {
          case PrimitiveType.PrimitiveTypeName.BINARY => creader.getBinary
          case PrimitiveType.PrimitiveTypeName.BOOLEAN => creader.getBoolean
          case PrimitiveType.PrimitiveTypeName.DOUBLE => {
            val x = creader.getDouble
            this._sum+=x.toLong
            x
          }
          case PrimitiveType.PrimitiveTypeName.FLOAT => creader.getFloat
          case PrimitiveType.PrimitiveTypeName.INT64 => {
            val x = creader.getLong
            this._sum+=x
            x
          }
          case PrimitiveType.PrimitiveTypeName.INT32 => {
            val x = creader.getInteger
            val y = matchType match {
              case PrimitiveType.PrimitiveTypeName.DOUBLE => {
                val xx = BigDecimal(x, 2).toDouble
                this._validDecimal+=1
                //println(" int32 -> double " + xx)
                this._sum+=xx.toLong
                xx
              }
              case _ => {
                this._sum+=x
                x
              }
            }
            y
          }

          case PrimitiveType.PrimitiveTypeName.INT96 => {
            val x = creader.getBinary.getBytesUnsafe
            if(x == null) null else new BigInteger(x)
          }
          case PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => {
            val y = creader.getBinary
            val x = y.getBytesUnsafe
            if(x == null)
              null
            else{
              val buffer = Charset.forName("UTF-8").newDecoder().decode(y.toByteBuffer)
              buffer.toString
            }
          }
          case _ => throw new Exception(" type did not match yet? " + column.getType)
        }
      }
      creader.consume()
    }
    //println( "rowX is " + rowsX + " rows " + rows)
    rows
  }
}

object ParquetReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new ParquetReadTest
}