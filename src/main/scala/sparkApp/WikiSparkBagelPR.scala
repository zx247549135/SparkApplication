package sparkApp

import com.esotericsoftware.kryo.Kryo
import org.apache.spark._
import org.apache.spark.bagel.{Bagel, Combiner, Message, Vertex}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.serializer.KryoRegistrator

/**
* Created by root on 15-9-19.
*/

class PageRankUtils extends Serializable {
  def computeWithCombiner(numVertices: Long, epsilon: Double)(
    self: PRVertex, messageSum: Option[Double], superstep: Int
    ): (PRVertex, Array[PRMessage]) = {
    val newValue = messageSum match {
      case Some(msgSum) if msgSum != 0 =>
        0.15  + 0.85 * msgSum
      case _ => self.value
    }

    val terminate = superstep >= 10

    val outbox: Array[PRMessage] =
      if (!terminate) {
        self.outEdges.map(targetId => new PRMessage(targetId, newValue / self.outEdges.size))
      } else {
        Array[PRMessage]()
      }
    val testPR = new PRVertex(newValue, self.outEdges, !terminate)
    (new PRVertex(newValue, self.outEdges, !terminate), outbox)
  }

  def computeNoCombiner(numVertices: Long, epsilon: Double)
                       (self: PRVertex, messages: Option[Array[PRMessage]], superstep: Int)
  : (PRVertex, Array[PRMessage]) =
    computeWithCombiner(numVertices, epsilon)(self, messages match {
      case Some(msgs) => Some(msgs.map(_.value).sum)
      case None => None
    }, superstep)
}

class PRCombiner extends Combiner[PRMessage, Double] with Serializable {
  def createCombiner(msg: PRMessage): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b
}

class PRVertex() extends Vertex with Serializable {
  var value: Double = _
  var outEdges: Array[Int] = _
  var active: Boolean = _

  def this(value: Double, outEdges: Array[Int], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "PRVertex(value=%f, outEdges.length=%d, active=%s)"
      .format(value, outEdges.length, active.toString)
  }
}

class PRMessage() extends Message[Int] with Serializable {
  var targetId: Int = _
  var value: Double = _

  def this(targetId: Int, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }

  override def toString():String={
    "PRMessage(targetId="+targetId+", value="+value+")"
  }
}

class PRKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex])
    kryo.register(classOf[PRMessage])
  }
}

class CustomPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = {
    val hash = key match {
      case k: Long => (k & 0x00000000FFFFFFFFL).toInt
      case _ => key.hashCode
    }

    val mod = key.hashCode % partitions
    if (mod < 0) mod + partitions else mod
  }

  override def equals(other: Any): Boolean = other match {
    case c: CustomPartitioner =>
      c.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions
}

object WikiSparkBagelPR {

  val ordering = implicitly[Ordering[Int]]

  def main(args:Array[String]): Unit ={

    if(args.length < 5){
      System.err.println("Usage:WikipediaPageRank <inputFile> <threshold> <numPartitions> <usePartitioner> <appName>")
      System.exit(-1)
    }

    val sparkConf = new SparkConf().setAppName(args(5)).
      set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator",classOf[PRKryoRegistrator].getName)

    val threshold = args(1).toDouble
    val numPartitions = args(2).toInt
    val usePartitioner = args(3).toBoolean

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
      (id,new PRVertex(1.0, list))
    })
    if (usePartitioner) {
      vertices = vertices.partitionBy(new HashPartitioner(numPartitions)).cache
    } else {
      vertices = vertices.cache
    }
    println("Done parsing input file.")

     // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(Int, PRMessage)]())
    val utils = new PageRankUtils
      val result =
        Bagel.run(
          sc, vertices, messages, combiner = new PRCombiner(),
          numPartitions = numPartitions)(
            utils.computeWithCombiner(numVertices, epsilon))

    result.mapValues(_.value).saveAsTextFile(args(4))
    //result.foreach(t => println(t.toString()))
  }
}
