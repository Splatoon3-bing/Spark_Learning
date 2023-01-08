package WorkSolution

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object InsertOverwritePar {
  val conf = new SparkConf().setMaster("local[*]").setAppName("InsertOverwritePar")
  val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  val df1: DataFrame = session.sql("select * from table")
  session.sql(
    """
      |Alter table table1 drop if exists partition ( a = 'value1',b = 'value2')
      |""".stripMargin)
  df1.write.mode("overwrite").format("orc").partitionBy("a","b").saveAsTable("table1")//要求事先存在这个分区
  session.sql(
    """
      |alter table table1
      | add partition(partition_key1 = "value1",partition_key2 = "value2")
      |""".stripMargin)

  df1.write.mode("overwrite").insertInto("table1")//向已有分区插入数据
  //上面两种方法都不好，都会先删除整个表，其他分区也会被影响,而且实现还要存在这个分区


  //这下面两种方法也不好，虽然他们可以不影响其他分区，但是不支持任务的重调
  df1.write.mode("append").format("orc").partitionBy("a","b").saveAsTable("table1")
  df1.write.mode("dynamic").format("orc").partitionBy("a","b").saveAsTable("table1")
  //对上面进行改进
  //df1.dropDuplicates("a","b","c","d")


  //那使用SparkSql进行操作呢？和HiveSql如出一辙
  session.sql(
    """
      |insert over write table
      |   table1
      |partiton
      |(a = 'value1'
      |b = 'value2')
      |""".stripMargin)//这个方法也有问题，如果没有目标分区的话会报错，所以得先新建一个分区，这点hivesql就做的很好，会自动新建一个分区
  session.sql("alter table table1 add partition ( a = 'value' ,  b = 'value1')")//新建一个分区
  session.sql("alter table table1 drop partition ( a = 'value' ,  b = 'value1')")//删除一个分区
  session.sql("show partition table1")
}
