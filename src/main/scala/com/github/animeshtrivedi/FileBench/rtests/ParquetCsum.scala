package com.github.animeshtrivedi.FileBench.rtests

import java.nio.ByteBuffer

import com.github.animeshtrivedi.FileBench.{AbstractTest, DumpGroupConverterX, JavaUtils, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}
import org.apache.parquet.schema.{MessageType, OriginalType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
  * Created by atr on 22.08.18.
  *
  * Derived from ParquetReadTest
  */
class ParquetCsum extends AbstractTest {
  private[this] var parquetFileReader:ParquetFileReader = _
  private[this] var schema:MessageType = _
  private[this] var mdata:FileMetaData = _

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val readFooter:ParquetMetadata = ParquetFileReader.readFooter(conf,
      path,
      ParquetMetadataConverter.NO_FILTER)
    this.mdata = readFooter.getFileMetaData
    this.schema = mdata.getSchema
    this.parquetFileReader = new ParquetFileReader(conf,
      this.mdata,
      path,
      readFooter.getBlocks,
      this.schema.getColumns)
    this.bytesOnFS = expectedBytes
  }

  final override def run(): Unit = {
    var pageReadStore:PageReadStore = null
    val colDesc = schema.getColumns
    val size = colDesc.size()

    val conv = new DumpGroupConverterX
    val s2 = System.nanoTime()
    try
    {
      pageReadStore = parquetFileReader.readNextRowGroup()
      while (pageReadStore != null) {
        val colReader = new ColumnReadStoreImpl(pageReadStore, conv,
          schema, mdata.getCreatedBy)
        var i = 0
        while (i < size){
          val col = colDesc.get(i)
          // parquet also encodes what was the original type of the filed vs what it is saved as
          val orginal = this.schema.getFields.get(i).getOriginalType
          col.getType match {
            case PrimitiveTypeName.INT32 => consumeIntColumn(colReader, col, Option(orginal), i)
            case PrimitiveTypeName.INT64 => consumeLongColumn(colReader, col, Option(orginal), i)
            case PrimitiveTypeName.DOUBLE => consumeDoubleColumn(colReader, col, Option(orginal), i)
            case PrimitiveTypeName.BINARY => consumeBinaryColumn(colReader, col, Option(orginal), i)
            case _ => throw new Exception(" NYI " + col.getType)
          }
          i+=1
        }
        this.totalRows+=pageReadStore.getRowCount
        pageReadStore = parquetFileReader.readNextRowGroup()
      }
    } catch {
      case foo: Exception => foo.printStackTrace()
    } finally {
      val s3 = System.nanoTime()
      this.runTimeInNanoSecs = s3 - s2
      parquetFileReader.close()
      printStats()
    }
  }
  private [this] def consumeIntColumn(crstore: ColumnReadStoreImpl,
                                      column: org.apache.parquet.column.ColumnDescriptor,
                                      original:Option[OriginalType],
                                      index:Int): Long = {
    require(original.isEmpty || (original.get == OriginalType.DATE))
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val x= creader.getInteger
        // integer checksum is generated as it is
        this._sum+=x
        this._validInt+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }


  private [this] def consumeBinaryColumn(crstore: ColumnReadStoreImpl,
                                         column: org.apache.parquet.column.ColumnDescriptor,
                                         original:Option[OriginalType],
                                         index:Int): Long = {
    require(original.isEmpty || (original.get == OriginalType.UTF8))
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val binArray = creader.getBinary.getBytes
        val binLength = binArray.length
        var arrayCheckum:Long = 0
        var j : Int = 0
        while (j < binLength){
          arrayCheckum = arrayCheckum + binArray(j)
          j= j + 1
        }
        this._sum+= (binLength + arrayCheckum)
        this._validBinarySize+=binLength
        this._validBinary+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }

  private [this] def consumeLongColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor,
                                       original:Option[OriginalType],
                                       index:Int): Long = {
    require(original.isEmpty)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        this._sum+=creader.getLong
        this._validLong+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }

  private [this] def consumeDoubleColumn(crstore: ColumnReadStoreImpl,
                                         column: org.apache.parquet.column.ColumnDescriptor,
                                         original:Option[OriginalType],
                                         index:Int): Long = {
    require(original.isEmpty)
    val doubleByteBuffer:ByteBuffer = ByteBuffer.allocate(java.lang.Double.BYTES)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    var i = 0L
    while (i < rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val d = creader.getDouble
        doubleByteBuffer.putDouble(0, d)
        // checked on the scala prompt, 2 buffers with the same double value give the same checksum
        this._sum+=doubleByteBuffer.hashCode()
        this._validDouble+=1
      }
      creader.consume()
      i+=1L
    }
    rows
  }
}

object ParquetCsum extends TestObjectFactory {
  final override def allocate(): AbstractTest = new ParquetCsum
}
