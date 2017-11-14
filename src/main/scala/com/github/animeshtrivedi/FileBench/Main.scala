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
    val x = new SFFTest(options.getInputDir, options.getParallel)
  }
}
