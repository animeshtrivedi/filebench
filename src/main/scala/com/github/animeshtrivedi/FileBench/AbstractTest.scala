package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 14.11.17.
  */
case class TestResult(rows:Long, bytes:Long, runtimeNanoSec:Long)

trait AbstractTest extends Runnable with Serializable {
  def init(fileName:String, expectedBytes:Long)
  def getResults():TestResult
}
