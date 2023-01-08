package Test

import org.apache.spark.{SparkConf, SparkContext}

object rddtest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkContext = new SparkContext(conf)
    val value = sparkContext.textFile("datas/input", 4)
    value.saveAsTextFile("datas/out")
  }

}
