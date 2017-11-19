package com.github.animeshtrivedi.FileBench.tests

import com.github.animeshtrivedi.FileBench.{AbstractTest, TestObjectFactory, TestResult}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{DecimalColumnVector, LongColumnVector, VectorizedRowBatch}
import org.apache.orc.{OrcFile, RecordReader}

/**
  * Created by atr on 19.11.17.
  */
class ORCReadTest extends  AbstractTest {

  private[this] var runTime = 0L
  private[this] var totalBytesExpected = 0L
  private[this] var totalRows = 0L
  private[this] var rows:RecordReader = _
  private[this] var batch:VectorizedRowBatch = _

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


  final override def run(): Unit = {
    var rowCount = 0L
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

        val intVal10 = intVector10.vector(i)

        val decimalVal0 = decimalVector0.vector(i)
        val decimalVal1 = decimalVector1.vector(i)
        val decimalVal2 = decimalVector2.vector(i)
        val decimalVal3 = decimalVector3.vector(i)
        val decimalVal4 = decimalVector4.vector(i)
        val decimalVal5 = decimalVector5.vector(i)
        val decimalVal6 = decimalVector6.vector(i)
        val decimalVal7 = decimalVector7.vector(i)
        val decimalVal8 = decimalVector8.vector(i)
        val decimalVal9 = decimalVector9.vector(i)
        val decimalVal10 = decimalVector10.vector(i)
        val decimalVal11 = decimalVector11.vector(i)

        rowCount += 1
      }
    }
    val s3 = System.nanoTime()
    rows.close()
    this.runTime = s3 - s2
  }
}

object ORCReadTest extends TestObjectFactory{
  final override def allocate(): AbstractTest = new ORCReadTest
}
