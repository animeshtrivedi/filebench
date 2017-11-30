package com.github.animeshtrivedi.FileBench.rtests

import java.util

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups
import org.apache.parquet.filter2.predicate.FilterApi.intColumn
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, ParquetMetadata}
import org.apache.parquet.io.api.{GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, OriginalType}

/**
  * Created by atr on 19.11.17.
  */
//    println(this.schema)
//    for (i <- 0 until this.schema.getColumns.size()){
//      val ty = this.schema.getType(i)
//      require(ty.isInstanceOf[PrimitiveType])
//      val ot = ty.getOriginalType
//      println( this.schema.getType(i) + " original " + ot)
//      if(ot != null){
//        ot match {
//          case OriginalType.DECIMAL => println(" decimal match" + this.schema.getType(i).asPrimitiveType().getDecimalMetadata)
//          case _ => println(" I don't kow ")
//        }
//      }
//    }

class ParquetReadTest extends AbstractTest {

  private[this] var parquetFileReader:ParquetFileReader = _
  private[this] var schema:MessageType = _
  private[this] var mdata:FileMetaData = _

  private[this] def generateFilters(gen:Boolean, footer:ParquetMetadata):util.List[BlockMetaData] = {
    if(gen){
      val maxFilter = FilterApi.gt(intColumn("value"),
        1945720194.asInstanceOf[java.lang.Integer])

      val minFilter = FilterApi.lt(intColumn("value"),
        -1832563198.asInstanceOf[java.lang.Integer])

      val filterPredicate = FilterApi.or(maxFilter, minFilter)
      val filter = FilterCompat.get(filterPredicate, null)
      // at this point we have all the details
      import org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups
      filterRowGroups(filter, footer.getBlocks, footer.getFileMetaData.getSchema)
    } else {
      footer.getBlocks
    }
  }

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    /* notice this is metadata filter - I do not know where to put the data filter yet */
    val readFooter:ParquetMetadata = ParquetFileReader.readFooter(conf,
      path,
      ParquetMetadataConverter.NO_FILTER)
    this.mdata = readFooter.getFileMetaData
    this.schema = mdata.getSchema
    this.parquetFileReader = new ParquetFileReader(conf,
      this.mdata,
      path,
      generateFilters(false, readFooter),
      this.schema.getColumns)
    // I had this before which does not support putting filters - ParquetFileReader.(conf, path).
    println( " expected in coming records are : " + parquetFileReader.getRecordCount)
    this.readBytes = expectedBytes
  }

  final override def run(): Unit = {
    var pageReadStore:PageReadStore = null
    val colDesc = schema.getColumns
    val size = colDesc.size()

    val conv = new DumpGroupConverter
    val s2 = System.nanoTime()
    try
    {
      pageReadStore = parquetFileReader.readNextRowGroup()
      while (pageReadStore != null) {
        val colReader = new ColumnReadStoreImpl(pageReadStore, conv,
          schema, mdata.getCreatedBy)
        for(i <-0 until size){
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

  private [this] def _consumeIntColumn(crstore: ColumnReadStoreImpl,
                                      column: org.apache.parquet.column.ColumnDescriptor):Long = {
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax) {
        this._sum +=creader.getInteger
      }
      creader.consume()
    }
    ???
    rows
  }

  private [this] def _consumeInt2DecimalColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor, index:Int):Long = {
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val dd = this.schema.getType(index).asPrimitiveType().getDecimalMetadata
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax) {
        val doubleVal = BigDecimal(creader.getInteger, dd.getScale).toDouble
        this._sum +=doubleVal.toLong
        this._validDecimal+=1
      }
      creader.consume()
    }
    ???
    rows
  }

  private [this] def _consumeIntColumn(crstore: ColumnReadStoreImpl,
                                      column: org.apache.parquet.column.ColumnDescriptor,
                                      original:Option[OriginalType],
                                      index:Int): Long = {
    original match {
      case Some(i) => i match {
        case OriginalType.DECIMAL => _consumeInt2DecimalColumn(crstore, column, index)
        case _=> throw new Exception()
      }
      case None => _consumeIntColumn(crstore, column)
    }
  }

  private [this] def consumeIntColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor,
                                       original:Option[OriginalType],
                                       index:Int): Long = {
    require(original.isEmpty)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val x= creader.getInteger
        println( " int value as " + x + " > " + creader.getDescriptor.getPath.foreach(print))
        this._sum+=x
        this._validInt+=1
        if(this._validInt > 100)
          ???
      }
      creader.consume()
    }
    rows
  }

  private [this] def consumeBinaryColumn(crstore: ColumnReadStoreImpl,
                                      column: org.apache.parquet.column.ColumnDescriptor,
                                      original:Option[OriginalType],
                                      index:Int): Long = {
    require(original.isEmpty)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        val bin = creader.getBinary
        this._sum+=bin.length()
        this._validBinarySize+=bin.length()
        this._validBinary+=1
      }
      creader.consume()
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
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        this._sum+=creader.getLong
        this._validLong+=1
      }
      creader.consume()
    }
    rows
  }

  private [this] def consumeDoubleColumn(crstore: ColumnReadStoreImpl,
                                       column: org.apache.parquet.column.ColumnDescriptor,
                                       original:Option[OriginalType],
                                       index:Int): Long = {
    require(original.isEmpty)
    val dmax = column.getMaxDefinitionLevel
    val creader:ColumnReader = crstore.getColumnReader(column)
    val rows = creader.getTotalValueCount
    for (i <- 0L until rows){
      val dvalue = creader.getCurrentDefinitionLevel
      if(dvalue == dmax){
        this._sum+=creader.getDouble.toLong
        this._validDouble+=1
      }
      creader.consume()
    }
    rows
  }

  private [this] class DumpGroupConverter extends GroupConverter {
    final def start() {}
    final def end() {}
    final def getConverter(fieldIndex: Int) = new DumpConverter
  }

  private [this] class DumpConverter extends PrimitiveConverter {
    final override def asGroupConverter = new DumpGroupConverter
  }
}

object ParquetReadTest extends TestObjectFactory {
  final override def allocate(): AbstractTest = new ParquetReadTest
}