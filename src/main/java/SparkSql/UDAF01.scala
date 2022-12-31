package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}


object UDAF01 {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))//注册我们的方式

    spark.sql("select ageAvg(age) from user").show


    // TODO 关闭环境
    spark.close()
  }

  /*
   自定义聚合函数类：计算年龄的平均值
   1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
       IN : 输入的数据类型 Long
       BUF : 缓冲区的数据类型 Buff
       OUT : 输出的数据类型 Long
   2. 重写方法(6)
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // z & zero : 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product//固定写法，自定义的类就写product,因为buff是我们自定义的类

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong//固定写法,scala自带的我们就写scalaLong
  }

}
