package com.github.animeshtrivedi.FileBench.tests

import java.math.BigInteger
import java.nio.charset.Charset

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}
import org.apache.parquet.io.api.{GroupConverter, PrimitiveConverter}
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

  override def init(fileName: String, expectedBytes: Long): Unit = {
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

  override def getResults(): TestResult = TestResult(expectedRows, this.readBytes, runTime)

  override def run(): Unit = {
    var tempRows = 0L
    var rowBatchesx = 0L
    var readSoFarRows = 0L
    var pageReadStore:PageReadStore = null
    val colDesc = schema.getColumns
    val s2 = System.nanoTime()
    try
    {
      var contx = true
      while (contx) {
        pageReadStore = parquetFileReader.readNextRowGroup()
        if (pageReadStore != null) {
          rowBatchesx+=1
          val colReader = new ColumnReadStoreImpl(pageReadStore, new DumpGroupConverter,
            schema, mdata.getCreatedBy)
          for(i <-0 until colDesc.size()){
            tempRows += consumeColumn(colReader, colDesc.get(i))
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
    }
  }

  private [this] class DumpGroupConverter extends GroupConverter {
    def start() {}
    def end() {}
    def getConverter(fieldIndex: Int) = new DumpConverter
  }

  private [this] class DumpConverter extends PrimitiveConverter {
    override def asGroupConverter = new DumpGroupConverter
  }

  private [this] def consumeColumn(crstore: ColumnReadStoreImpl, column: org.apache.parquet.column.ColumnDescriptor): Long = {
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val rvalue = creader.getCurrentRepetitionLevel
      val dvalue = creader.getCurrentDefinitionLevel
      if(rvalue == dvalue) {
        val value = column.getType match {
          case PrimitiveType.PrimitiveTypeName.BINARY => creader.getBinary
          case PrimitiveType.PrimitiveTypeName.BOOLEAN => creader.getBoolean
          case PrimitiveType.PrimitiveTypeName.DOUBLE => creader.getDouble
          case PrimitiveType.PrimitiveTypeName.FLOAT => creader.getFloat
          case PrimitiveType.PrimitiveTypeName.INT64 => creader.getLong
          case PrimitiveType.PrimitiveTypeName.INT32 => creader.getInteger
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
        }
      }
      creader.consume()
    }
    rows
  }
}
