package com.lw.scala.myflink.batch

object Test {
  def main(args:Array[String]): Unit ={
    val ary=Array(1,2,3,4)
    ary.map(e=>e*e)
      .foreach(println(_))
  }
}
