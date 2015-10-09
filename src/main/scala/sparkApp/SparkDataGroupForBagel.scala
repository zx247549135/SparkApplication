package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-27.
 */
object SparkDataGroupForBagel {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkDataGroupForBagel")
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0))
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0),parts(1))
    }.groupByKey().map{ line =>
      val key = line._1
      val value = line._2.mkString(" ")
      key+" "+value
    }
    links.saveAsTextFile(args(1))
  }
}
