package com.github.animeshtrivedi.FileBench

import com.github.animeshtrivedi.FileBench.tests.{HdfsReadTest, ParquetReadTest}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.simplefileformat.SimpleFileFormat

/**
  * Created by atr on 14.11.17.
  */
object Utils {

  val SFFMetadataExtension:String = SimpleFileFormat.metadataExtension

  def isSFFMetaFile(path:String):Boolean = {
    SimpleFileFormat.isStringPathMetaFile(path)
  }

  def ok(path:Path):Boolean = {
    val fname = path.getName
    fname(0) != '_' && fname(0) != '.' && !isSFFMetaFile(fname)
  }

  def enumerateWithSize(fileName:String):List[(String, Long)] = {
    if(fileName != null) {
      val path = new Path(fileName)
      val conf = new Configuration()
      val fileSystem = path.getFileSystem(conf)
      // we get the file system
      val fileStatus: Array[FileStatus] = fileSystem.listStatus(path)
      val files = fileStatus.map(_.getPath).filter(ok).toList
      files.map(fx => (fx.toString, fileSystem.getFileStatus(fx).getLen))
    } else {
      /* this will happen for null io */
      List[(String, Long)]()
    }
  }

  def fromStringToFactory(str:String):TestObjectFactory = {
    str.toLowerCase() match {
      case "hdfsread" => HdfsReadTest
      case "parquetread" => ParquetReadTest
      case _ => throw new Exception(" whoa ...")
    }
  }
}
