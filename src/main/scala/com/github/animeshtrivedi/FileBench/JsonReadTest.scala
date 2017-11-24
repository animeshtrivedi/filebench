package com.github.animeshtrivedi.FileBench

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
  * Created by atr on 23.11.17.
  */
class JsonReadTest extends AbstractTest {
  private[this] var jsonParser:JsonParser = _

  override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    val path = new Path(fileName)
    val fileSystem = path.getFileSystem(conf)
    val instream = fileSystem.open(path)
    this.readBytes = expectedBytes
    this.jsonParser = new JsonFactory().createParser(instream)
  }

  override def run(): Unit = {
    throw new Exception(" this implementation is not verified ")
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
    this.runTimeInNanoSecs = System.nanoTime() - s
    printStats()
    println(" ********** DOUBLE does not exhist here? ")
  }
}

object JsonReadTest extends TestObjectFactory {
  override def allocate(): AbstractTest = new JsonReadTest
}
