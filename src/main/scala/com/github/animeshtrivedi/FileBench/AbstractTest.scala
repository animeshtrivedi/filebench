package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 14.11.17.
  */
case class TestResult(rows:Long, bytes:Long, runtimeNanoSec:Long) {
  override def toString:String = "[TestResult] rows: " + rows + " bytes " + bytes + " runTimeinNanosec " + runtimeNanoSec
}


abstract class AbstractTest extends Runnable with Serializable {
  protected var _sum:Long = 0L
  protected var _validDecimal:Long = 0L
  protected var _validInt:Long = 0L
  protected var _validLong:Long = 0L
  protected var _validDouble:Long = 0L

  protected var runTimeInNanoSecs = 0L
  protected var readBytes = 0L
  protected var totalRows = 0L


  protected def printStats(){
    println("sum: " + this._sum +
      " | ints: " + this._validInt +
      " longs: " + this._validLong +
      " doubles:" + this._validDouble +
      " decimals: " + this._validDecimal)
  }

  final def getTotalSizeInBytes:Long = {
    (this._validInt * java.lang.Integer.BYTES) +
      (this._validLong * java.lang.Long.BYTES) +
      (this._validDouble * java.lang.Double.BYTES)
  }

  final def getResults:TestResult = TestResult(this.totalRows, this.readBytes, this.runTimeInNanoSecs)

  def init(fileName:String, expectedBytes:Long)
}
