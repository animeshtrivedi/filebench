package com.github.animeshtrivedi.FileBench

import java.net.URI

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal

/**
  * Created by atr on 19.11.17.
  */
//https://www.ctheu.com/2017/03/02/serializing-data-efficiently-with-apache-avro-and-dealing-with-a-schema-registry/
//https://www.tutorialspoint.com/avro/serialization_by_generating_class.htm
class AvroReadTest extends  AbstractTest {
  // we can found this from the metadata file of the SFF files
  private[this] val schemaStr = "{\"type\":\"struct\",\"fields\":[{\"name\":\"ss_sold_date_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_sold_time_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_item_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_customer_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_cdemo_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_hdemo_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_addr_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_store_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_promo_sk\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ticket_number\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_quantity\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_wholesale_cost\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_list_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_discount_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_wholesale_cost\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_list_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_ext_tax\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_coupon_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_net_paid\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_net_paid_inc_tax\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ss_net_profit\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}}]}"
  private[this] val userProvidedSchema = Some(new Schema.Parser().parse(schemaStr))
  private[this] var reader:FileReader[GenericRecord] = _
  private[this] var totalBytes = 0L
  private[this] var totalRows = 0L
  private[this] var runTime = 0L

  final override def init(fileName: String, expectedBytes: Long): Unit = {
    val conf = new Configuration()
    this.reader = {
      val in = new FsInput(new Path(new URI(fileName)), conf)
      try {
        val datumReader = userProvidedSchema match {
          case Some(userSchema) => new GenericDatumReader[GenericRecord](userSchema)
          case _ => new GenericDatumReader[GenericRecord]()
        }
        DataFileReader.openReader(in, datumReader)
      } catch {
        case NonFatal(e) =>
          in.close()
          throw e
      }
    }
    this.reader.sync(0)
    this.totalBytes = expectedBytes
  }

  final override def getResults(): TestResult = TestResult(this.totalRows, this.totalBytes, this.runTime)

  final override def run(): Unit = {
    /* top level loop */
    val fx = userProvidedSchema.get.getFields
    val s1 = System.nanoTime()
    while(reader.hasNext && !reader.pastSync(this.totalBytes)){
      val record = reader.next().asInstanceOf[GenericRecord]
      for( i <- 0 until fx.size()){
        /* materialize */
        val x= record.get(fx.get(i).name())
      }
      totalRows+=1
    }
    val s2 = System.nanoTime()
    this.runTime = s2 - s1
    reader.close()
  }
}

object AvroReadTest extends TestObjectFactory {
  override def allocate(): AbstractTest = new AvroReadTest
}


