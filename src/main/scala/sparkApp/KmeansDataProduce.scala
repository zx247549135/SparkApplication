package sparkApp


import breeze.linalg.DenseVector
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by root on 15-10-10.
 */

class ObjectDenseVector(denseVector: DenseVector[Double]) extends Serializable{

  def getValue():DenseVector[Double] = this.denseVector

}

object KmeansDataProduce {

  val rand = new Random(42)

  def generateData(size:Int,numDest:Int): Array[ObjectDenseVector] = {
    def generatePoint(i: Int): ObjectDenseVector = {
      val test = if(i % 2 == 0) -1 else 1
      val x = DenseVector.fill(numDest){rand.nextGaussian + test*0.7}
      new ObjectDenseVector(x)
    }
    Array.tabulate(size)(generatePoint)
  }

  def main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName("KmeansDataProduce")
    val sc = new SparkContext(sparkConf)
    val numDest = args(0).toInt
    val numSimplesEach = args(1).toInt
    val numSimplesSlices = args(2).toInt
    val numSlices = args(3).toInt

    val result = sc.parallelize(0 until numSimplesSlices, numSlices).flatMap(i => generateData(numSimplesEach,numDest))

    result.saveAsObjectFile(args(4))

  }

}
