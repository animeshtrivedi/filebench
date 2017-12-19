package com.github.animeshtrivedi.FileBench.helper

import org.apache.parquet.io.api.PrimitiveConverter

/**
  * Created by atr on 19.12.17.
  */
class DumpConverter extends PrimitiveConverter {
  final override def asGroupConverter = new DumpGroupConverter
}
