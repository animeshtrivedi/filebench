package com.github.animeshtrivedi.FileBench

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 23.11.17.
  */
class JsonReadTest extends AbstractTest {
  private[this] var jsonParser:JsonParser = _
  private[this] var totalRows = 0L
  private[this] var runTime = 0L
  private[this] var totalBytes = 0L

  private[this] var _sum:Long = 0L
  private[this] var _validDecimal:Long = 0L

  override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    val instream = fileSystem.open(path)
    this.totalBytes = expectedBytes
    this.jsonParser = new JsonFactory().createParser(instream)
  }

  override def getResults(): TestResult = TestResult(this.totalRows, this.totalBytes ,this.runTime)

  override def run(): Unit = {
    val s = System.nanoTime()
    while(!jsonParser.isClosed){
      var token = jsonParser.nextToken()
      token match {
        case JsonToken.END_OBJECT => this.totalRows+=1
        case JsonToken.START_OBJECT => //println (" start object ")
        case JsonToken.FIELD_NAME => //println (" filed name as  " + jsonParser.getCurrentName)
        case JsonToken.VALUE_NUMBER_FLOAT => {
          //println (" float value " + jsonParser.getFloatValue)
          this._sum+=jsonParser.getFloatValue.toLong
          this._validDecimal+=1
        }
        case JsonToken.VALUE_NUMBER_INT => {
          //println (" int value " + jsonParser.getIntValue)
          this._sum+=jsonParser.getIntValue
        }
        case JsonToken.VALUE_NULL | null => //println(" NULL VALUE here ")
        case x:AnyRef => {
          println("**** " + x)
          throw new Exception
        }
      }
    }
    this.runTime = System.nanoTime() - s
    println(this._sum + " valid decimal " + this._validDecimal)
  }
}

object JsonReadTest extends TestObjectFactory {
  override def allocate(): AbstractTest = new JsonReadTest
}
