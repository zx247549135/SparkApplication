package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by root on 15-10-10.
 */
object KmeansDataProduce {

  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("KmeansDataProduce")
    val sc = new SparkContext(sparkConf)
    val numDest = args(0).toInt
    val numSimples = args(1).toInt
    val numSlices = args(2).toInt

    val rand = new Random(42)

    def generateData: Array[Array[Double]] = {
      def generatePoint(i: Int): Array[Double] = {
        Array.tabulate(numDest)(i => rand.nextGaussian)
      }
      Array.tabulate(numSimples)(generatePoint)
    }

    val data = sc.parallelize(generateData, numSlices)

    data.map(_.mkString(" ")).saveAsTextFile(args(3))

  }

}
