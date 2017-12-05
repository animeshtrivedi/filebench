package com.github.animeshtrivedi.FileBench.rtests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.orc.{OrcFile, RecordReader, TypeDescription}

/**
  * Created by atr on 19.11.17.
  *
  * https://codecheese.wordpress.com/2017/06/13/reading-and-writing-orc-files-using-vectorized-row-batch-in-java/
  */
class ORCReadTest extends  AbstractTest {
  private[this] var rows:RecordReader = _
  private[this] var schema:TypeDescription = _
  private[this] var batch:VectorizedRowBatch = _

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.readBytes = expectedBytes

    val conf: Configuration = new Configuration()
    val path = new Path(fileName)
    val reader = OrcFile.createReader(path,
      OrcFile.readerOptions(conf))
    this.rows = reader.rows()
    this.schema = reader.getSchema
    this.batch = this.schema.createRowBatch()
  }

  final private[this] def consumeIntColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val intVector: LongColumnVector = batch.cols(index).asInstanceOf[LongColumnVector]
    val isNull = intVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val intVal = intVector.vector(i)
        this._sum+=intVal
        this._validInt+=1
      }
    }
  }

  final private[this] def consumeDecimalColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val decimalVector: DecimalColumnVector = batch.cols(index).asInstanceOf[DecimalColumnVector]
    val isNull = decimalVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val decimalVal = decimalVector.vector(i).doubleValue()
        this._sum+=decimalVal.toLong
        this._validDecimal+=1
      }
    }
  }

  final private[this] def consumeLongColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val longVector: LongColumnVector = batch.cols(index).asInstanceOf[LongColumnVector]
    val isNull = longVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val longVal = longVector.vector(i)
        this._sum+=longVal
        this._validLong+=1
      }
    }
  }

  final private[this] def consumeDoubleColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val doubleVector: DoubleColumnVector = batch.cols(index).asInstanceOf[DoubleColumnVector]
    val isNull = doubleVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val doubleVal = doubleVector.vector(i)
        this._sum+=doubleVal.toLong
        this._validDouble+=1
      }
    }
  }

  final private[this] def consumeBinaryColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val binaryVector: BytesColumnVector = batch.cols(index).asInstanceOf[BytesColumnVector]
    val isNull = binaryVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val binaryVal = binaryVector.vector(i)
        //println(" size is " + binaryVal.length + " length " + binaryVector.length(i) + " start " + binaryVector.start(i) + " buffer size " + binaryVector.bufferSize() + " batchSzie " + batch.size)
        // semantics binaryVal.length = sum(batch.size of all elems  binaryVector.length(i)), start is where we can copy the data out.
        this._sum+=binaryVector.length(i)
        this._validBinary+=1
        this._validBinarySize+=binaryVector.length(i)
      }
    }
  }

  final override def run(): Unit = {
    val all = this.schema.getChildren
    val s1 = System.nanoTime()
    while (rows.nextBatch(batch)) {
      /* loop over all the columns */
      for (i <- 0 until all.size()) {
        all.get(i).getCategory match {
          case TypeDescription.Category.LONG => consumeLongColumn(batch, i)
          case TypeDescription.Category.INT => consumeIntColumn(batch, i)
          case TypeDescription.Category.DOUBLE =>  consumeDoubleColumn(batch, i)
          case TypeDescription.Category.BINARY =>  consumeBinaryColumn(batch, i)
          case _  => {
            println(all.get(i) + " does not match anything?, please add the use case" + all.get(i).getCategory )
            throw new Exception()
          }
        }
      }
      this.totalRows += batch.size
    }
    this.runTimeInNanoSecs = System.nanoTime() - s1
    rows.close()
    printStats()
  }
}

object ORCReadTest extends TestObjectFactory{
  final override def allocate(): AbstractTest = new ORCReadTest
}
