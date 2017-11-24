package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 14.11.17.
  */
case class TestResult(rows:Long, bytes:Long, runtimeNanoSec:Long)

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
  final def getResults:TestResult = TestResult(this.totalRows, this.readBytes, this.runTimeInNanoSecs)

  abstract def init(fileName:String, expectedBytes:Long)
}
