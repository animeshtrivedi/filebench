package com.github.animeshtrivedi.FileBench

/**
  * @author ${user.name}
  */
object Main {

  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def main(args : Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
    val options = new ParseOptions()
    options.parse(args)
//    val chx = if (args.length == 0) {
//      0
//    } else {
//      args(0).toInt
//    }
//    if(chx == 0){
//      val x = new SFFTest(options.getInputDir, options.getParallel)
//    } else {
//      val x = new SFFReadPattern(options.getInputDir, options.getParallel)
//    }

    new TestFrameWork(HdfsReadTest, options.getInputDir, options.getParallel)

  }
}
