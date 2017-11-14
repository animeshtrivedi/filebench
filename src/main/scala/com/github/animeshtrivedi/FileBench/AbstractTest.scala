package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 14.11.17.
  */
trait AbstractTest extends Runnable {
  def init(fileName:String, expectedBytes:Long)
  def getResults():(Long, Long) // returns rows and bytes
}
