package flintApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by root on 15-10-10.
 */

class KmeansChunk(size:Int,numDest:Int) extends ByteArrayOutputStream(size:Int) with Serializable{ self =>

  def getOriginal()= new Iterator[Array[Double]]{
    var offset = 0

    override def hasNext = offset < self.count

    override def next():Array[Double]={
      if(!hasNext) Iterator.empty.next()
      else{
        val result = new Array[Double](numDest)
        for(i <- 0 until numDest){
          result.update(i,WritableComparator.readDouble(buf,offset))
          offset += 8
        }
        result
      }
    }
  }

  def getPointStats(K:Int,kPoints:Array[Double]):Iterator[Array[Double]]={
    var offset = 0
    val result = new Array[Double](K*(numDest+1))

    var closestIndex = 0
    var minDistance = Double.PositiveInfinity
    var tmpOffset = 0
    var tmpDist = 0.0

    while(offset < self.count){

      closestIndex = 0
      minDistance = Double.PositiveInfinity
      tmpOffset = 0
      tmpDist = 0.0

      for( i <- 0 until K){
        tmpOffset = offset
        tmpDist = 0.0
        val tmpStartVertex = i*numDest
        for(j <- 0 until numDest){
          val tmpValue = WritableComparator.readDouble(buf,tmpOffset)-kPoints(tmpStartVertex+j)
          tmpDist += tmpValue*tmpValue
          tmpOffset += 8
        }
        if(tmpDist < minDistance){
          minDistance = tmpDist
          closestIndex = i
        }
      }

      val startVertex = closestIndex*(numDest+1)
      for(i <- 0 until numDest){
        result(startVertex+i) += WritableComparator.readDouble(buf,offset)
        offset += 8
      }
      result(startVertex+numDest) += 1
    }

    Iterator(result)

  }

}

object FlintKmeans {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist> <appName> <numDest>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(3))
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val K = args(1).toInt
    val convergeDist = args(2).toDouble
    val numDest = args(4).toInt

    val data = lines.mapPartitions(dIters => {
      val (dIter,dIter2) = dIters.duplicate
      val length = dIter2.length
      val chunk = new KmeansChunk(numDest*8*length,numDest)
      val dos = new DataOutputStream(chunk)
      while(dIter.hasNext){
        val line = dIter.next()
        val parts = line.split("\\s+")
        parts.map(t => dos.writeDouble(t.toDouble))
      }
      Iterator(chunk)
    },true).persist(StorageLevel.MEMORY_AND_DISK)

    data.foreach(_ => Unit)

    val kPointstmp = data.mapPartitions( dIter => {
      val chunk = dIter.next()
      chunk.getOriginal()
    },true).takeSample(withReplacement = false, K, 42).toArray
    var kPoints = new Array[Double](K*numDest)
    for(i <- 0 until K){
      for(j <- 0 until numDest) {
        kPoints(i * numDest + j) = kPointstmp(i)(j)
      }
    }

    var terminate = 1.0
    var step = 1

    while(terminate > convergeDist){

      val startTime = System.currentTimeMillis()

      val pointStats = data.mapPartitions( dIter => {
        val chunk = dIter.next()
        chunk.getPointStats(K,kPoints)
      },true).collect()

      val midTime = System.currentTimeMillis()
      println("the RDD action time is "+(midTime-startTime)/1000.0 +"s")

      val result = new Array[Double](K*(numDest+1))

      for(num <- 0 until pointStats.length){
        for(i <- 0 until K){
          val tmpStartVertex = i*(numDest+1)
          for(j <- 0 to numDest){
            result(tmpStartVertex+j) += pointStats(num)(tmpStartVertex+j)
          }
        }
      }
      val newPoints = new Array[Double](K*numDest)
      for(i <- 0 until K){
        val tmpStartVertex = i*(numDest+1)
        for(j <- 0 until numDest){
          newPoints(i*numDest+j) = result(tmpStartVertex+j)/result(tmpStartVertex+numDest)
        }
      }

      terminate = 0.0
      for(i <- 0 until K){
        val tmpStartVertex = i*numDest
        for(j <- 0 until numDest) {
          val tmpValue = kPoints(tmpStartVertex+j) - newPoints(tmpStartVertex+j)
          terminate += tmpValue*tmpValue
        }
      }

      kPoints = newPoints

      val endTime = System.currentTimeMillis()

      println("Finished iteration "+step+" (delta = " + terminate + ") while time is " + (endTime-startTime)/1000.0 + "s")
      step += 1
    }

    println("Final centers:")
    for(i <- 0 until K) {
      for (j <- 0 until numDest)
        print(kPoints(i * numDest + j) + ";")
      println()
    }
    sc.stop()
  }

}
