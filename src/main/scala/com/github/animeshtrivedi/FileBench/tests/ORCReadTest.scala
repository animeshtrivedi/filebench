package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, DecimalColumnVector, LongColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, RecordReader}

/**
  * Created by atr on 19.11.17.
  *
  * https://codecheese.wordpress.com/2017/06/13/reading-and-writing-orc-files-using-vectorized-row-batch-in-java/
  */
class ORCReadTest extends  AbstractTest {

  private[this] var runTime = 0L
  private[this] var totalBytesExpected = 0L
  private[this] var totalRows = 0L
  private[this] var rows:RecordReader = _
  private[this] var batch:VectorizedRowBatch = _
  private[this] var _sum:Long = 0
  private[this] var _validDecimal:Long = 0

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    this.totalBytesExpected = expectedBytes

    val conf: Configuration = new Configuration()
    val path = new Path(fileName)
    val reader = OrcFile.createReader(path,
      OrcFile.readerOptions(conf))
    this.rows = reader.rows()
    this.batch = reader.getSchema.createRowBatch()
  }

  final override def getResults(): TestResult = TestResult(this.totalRows,
    this.totalBytesExpected,
    this.runTime)

  private[this] def consumeIntColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val intVector: LongColumnVector = batch.cols(index).asInstanceOf[LongColumnVector]
    val isNull = intVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val intVal = intVector.vector(i)
        this._sum+=intVal
      }
    }
  }

  private[this] def consumeDecimalColumn(batch:VectorizedRowBatch, index:Int):Unit = {
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

  private[this] def consumeLongColumn(batch:VectorizedRowBatch, index:Int):Unit = {
    val longVector: LongColumnVector = batch.cols(index).asInstanceOf[LongColumnVector]
    val isNull = longVector.isNull
    for (i <- 0 until batch.size) {
      if(!isNull(i)){
        val longVal = longVector.vector(i)
        this._sum+=longVal
      }
    }
  }


  private[this] final def runV1(): Unit = {
    val s2 = System.nanoTime()
    while (rows.nextBatch(batch)) {
      val colsArr = batch.cols
      val intVector0: LongColumnVector = colsArr(0).asInstanceOf[LongColumnVector]
      val intVector1: LongColumnVector = colsArr(1).asInstanceOf[LongColumnVector]
      val intVector2: LongColumnVector = colsArr(2).asInstanceOf[LongColumnVector]
      val intVector3: LongColumnVector = colsArr(3).asInstanceOf[LongColumnVector]
      val intVector4: LongColumnVector = colsArr(4).asInstanceOf[LongColumnVector]
      val intVector5: LongColumnVector = colsArr(5).asInstanceOf[LongColumnVector]
      val intVector6: LongColumnVector = colsArr(6).asInstanceOf[LongColumnVector]
      val intVector7: LongColumnVector = colsArr(7).asInstanceOf[LongColumnVector]
      val intVector8: LongColumnVector = colsArr(8).asInstanceOf[LongColumnVector]
      // +9 ints = 9
      val intVector9: LongColumnVector = colsArr(9).asInstanceOf[LongColumnVector]
      // +1 long = 10
      val intVector10: LongColumnVector = colsArr(10).asInstanceOf[LongColumnVector]
      // +1 int = 11
      val decimalVector0: DecimalColumnVector = colsArr(11).asInstanceOf[DecimalColumnVector]
      val decimalVector1: DecimalColumnVector = colsArr(12).asInstanceOf[DecimalColumnVector]
      val decimalVector2: DecimalColumnVector = colsArr(13).asInstanceOf[DecimalColumnVector]
      val decimalVector3: DecimalColumnVector = colsArr(14).asInstanceOf[DecimalColumnVector]
      val decimalVector4: DecimalColumnVector = colsArr(15).asInstanceOf[DecimalColumnVector]
      val decimalVector5: DecimalColumnVector = colsArr(16).asInstanceOf[DecimalColumnVector]
      val decimalVector6: DecimalColumnVector = colsArr(17).asInstanceOf[DecimalColumnVector]
      val decimalVector7: DecimalColumnVector = colsArr(18).asInstanceOf[DecimalColumnVector]
      val decimalVector8: DecimalColumnVector = colsArr(19).asInstanceOf[DecimalColumnVector]
      val decimalVector9: DecimalColumnVector = colsArr(20).asInstanceOf[DecimalColumnVector]
      val decimalVector10: DecimalColumnVector = colsArr(21).asInstanceOf[DecimalColumnVector]
      val decimalVector11: DecimalColumnVector = colsArr(22).asInstanceOf[DecimalColumnVector]
      // +12 decimal = 23 columns
      for (i <- 0 until batch.size) {
        val intVal0 = intVector0.vector(i)
        val intVal1 = intVector1.vector(i)
        val intVal2 = intVector2.vector(i)
        val intVal3 = intVector3.vector(i)
        val intVal4 = intVector4.vector(i)
        val intVal5 = intVector5.vector(i)
        val intVal6 = intVector6.vector(i)
        val intVal7 = intVector7.vector(i)
        val intVal8 = intVector8.vector(i)
        val intVal9 = intVector9.vector(i)
        //intVector0.isNull(i)???

        val intVal10 = intVector10.vector(i)

        //decimalVector0.isNull(i) vector(i).
        val decimalVal0 = decimalVector0.vector(i).doubleValue()
        val decimalVal1 = decimalVector1.vector(i).doubleValue()
        val decimalVal2 = decimalVector2.vector(i).doubleValue()
        val decimalVal3 = decimalVector3.vector(i).doubleValue()
        val decimalVal4 = decimalVector4.vector(i).doubleValue()
        val decimalVal5 = decimalVector5.vector(i).doubleValue()
        val decimalVal6 = decimalVector6.vector(i).doubleValue()
        val decimalVal7 = decimalVector7.vector(i).doubleValue()
        val decimalVal8 = decimalVector8.vector(i).doubleValue()
        val decimalVal9 = decimalVector9.vector(i).doubleValue()
        val decimalVal10 = decimalVector10.vector(i).doubleValue()
        val decimalVal11 = decimalVector11.vector(i).doubleValue()

        this._sum+=intVal0
        this._sum+=intVal1
        this._sum+=intVal2
        this._sum+=intVal3
        this._sum+=intVal4
        this._sum+=intVal5
        this._sum+=intVal6
        this._sum+=intVal7
        this._sum+=intVal8
        this._sum+=intVal9
        this._sum+=intVal10
        this._sum+=decimalVal0.toLong
        this._sum+=decimalVal1.toLong
        this._sum+=decimalVal2.toLong
        this._sum+=decimalVal3.toLong
        this._sum+=decimalVal4.toLong
        this._sum+=decimalVal5.toLong
        this._sum+=decimalVal6.toLong
        this._sum+=decimalVal7.toLong
        this._sum+=decimalVal8.toLong
        this._sum+=decimalVal9.toLong
        this._sum+=decimalVal10.toLong
        this._sum+=decimalVal11.toLong

        this.totalRows += 1

      }
    }
    val s3 = System.nanoTime()
    rows.close()
    this.runTime = s3 - s2
    println(this._sum)
  }

  final override def run(): Unit = runV2()

  private[this] final def runV2(): Unit = {
    val s2 = System.nanoTime()
    while (rows.nextBatch(batch)) {
      consumeIntColumn(batch, 0)
      consumeIntColumn(batch, 1)
      consumeIntColumn(batch, 2)
      consumeIntColumn(batch, 3)
      consumeIntColumn(batch, 4)
      consumeIntColumn(batch, 5)
      consumeIntColumn(batch, 6)
      consumeIntColumn(batch, 7)
      consumeIntColumn(batch, 8)

      consumeLongColumn(batch, 9)

      consumeIntColumn(batch, 10)

      consumeDecimalColumn(batch, 11)
      consumeDecimalColumn(batch, 12)
      consumeDecimalColumn(batch, 13)
      consumeDecimalColumn(batch, 14)
      consumeDecimalColumn(batch, 15)
      consumeDecimalColumn(batch, 16)
      consumeDecimalColumn(batch, 17)
      consumeDecimalColumn(batch, 18)
      consumeDecimalColumn(batch, 19)
      consumeDecimalColumn(batch, 20)
      consumeDecimalColumn(batch, 21)
      consumeDecimalColumn(batch, 22)
      this.totalRows += batch.size

    }
    val s3 = System.nanoTime()
    rows.close()
    this.runTime = s3 - s2
    println(this._sum + " valid decimal " + this._validDecimal)
  }
}


object ORCReadTest extends TestObjectFactory{
  final override def allocate(): AbstractTest = new ORCReadTest
}
