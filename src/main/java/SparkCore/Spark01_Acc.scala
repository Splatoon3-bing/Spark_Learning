package SparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // reduce : 分区内计算，分区间计算
        val i: Int = rdd.reduce(_+_)
        println(i)
        var sum = 0//在driver端,
        rdd.foreach(
            num => {
                sum += num
            }
        )//在Executor执行！在executor执行，并没有传回来的操作，所以Driver端的sum依旧没变化。
        println("sum = " + sum)

        sc.stop()

    }
}
