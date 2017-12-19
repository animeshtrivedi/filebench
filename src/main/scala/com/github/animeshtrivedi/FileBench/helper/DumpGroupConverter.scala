package com.github.animeshtrivedi.FileBench.helper

import org.apache.parquet.io.api.GroupConverter

/**
  * Created by atr on 19.12.17.
  */
class DumpGroupConverter extends GroupConverter {
  final def start() {}
  final def end() {}
  final def getConverter(fieldIndex: Int) = new DumpConverter
}
