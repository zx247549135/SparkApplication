package sparkApp

import org.apache.spark.bagel.Bagel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by root on 15-10-11.
 */
object SparkBagelCCSer {
  def main(args:Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName(args(3)).
      set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator",classOf[PRKryoRegistrator].getName)

    val threshold = 0.1
    val numPartitions = args(1).toInt
    val usePartitioner = true

    val sc = new SparkContext(sparkConf)

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(args(0),numPartitions)

    println("Counting vertices...")
    val numVertices = 1001
    println("Done counting vertices.")

    println("Spark:Parsing input file...")

    var vertices = input.map( s => {
      val fields = s.split("\\s+")
      val title = fields(0).toInt
      val target = fields(1).toInt
      (title,target)
    }).groupByKey().map(lines => {
      val id = lines._1
      val list = lines._2.toArray
      (id,new CCVertex(id, list))
    })
    if (usePartitioner) {
      vertices = vertices.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      vertices = vertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(Int, CCMessage)]())
    val utils = new CCUtils
    val result =
      Bagel.run(
        sc, vertices, messages, combiner = new CCCombiner(),
        numPartitions = numPartitions,StorageLevel.MEMORY_AND_DISK_SER)(
          utils.computeWithCombiner(numVertices, epsilon))

    result.mapValues(_.value).saveAsTextFile(args(2))
    val count = result.mapValues(_.value).values.distinct().count()
    println("the count of connected components is "+count)
    //result.foreach(t => println(t.toString()))
  }
}
