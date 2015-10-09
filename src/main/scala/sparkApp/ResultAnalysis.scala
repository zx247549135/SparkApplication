package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-29.
 */
object ResultAnalysis {
  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("ResultAnalysis")
    val sparkContext = new SparkContext(sparkConf)
    val linesA = sparkContext.textFile(args(0))
    val linesB = sparkContext.textFile(args(1))

    println("1.Count:"+linesA.count())
    println("2.Count:"+linesB.count())

  }
}
