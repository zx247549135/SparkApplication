package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-26.
 */
object SparkSQL {

  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName(args(3))
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val finalWords = lines.map(line => {
      val parts = line.split(",")
      (parts(0),parts(1).toDouble)
    }).filter(_._2 > args(2).toDouble)
    finalWords.saveAsTextFile(args(1))
  }

}
