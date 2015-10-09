package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-27.
 */
object SparkDataFilterForBagel {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkDataFilterForBagel").setMaster("local")
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0))
    val links = lines.filter { s =>
      val parts = s.split("\\s+")
      if(parts.length == 1)
        false
      else true
    }
    links.saveAsTextFile(args(1))
  }
}
