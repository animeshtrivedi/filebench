package com.github.animeshtrivedi.FileBench

/**
  * Created by atr on 17.11.17.
  */
class HdfsWriteTest extends AbstractTest {
  override def init(fileName: String, expectedBytes: Long): Unit = ???

  override def getResults(): (Long, Long, Long) = ???

  override def run(): Unit = ???
}

object HdfsWriteTest extends AllocateTestObject {
  final override def allocate(): AbstractTest = new HdfsWriteTest
}
