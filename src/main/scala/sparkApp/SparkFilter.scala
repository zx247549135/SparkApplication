package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-26.
 */
object SparkFilter {

  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val finalWords = lines.flatMap(s => {
      val parts = s.split("\\s+")
      parts
    }).filter(_.endsWith(args(2)))
    finalWords.saveAsTextFile(args(1))
  }

}
