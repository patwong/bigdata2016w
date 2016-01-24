package ca.uwaterloo.cs.bigdata2016w.patwong.assignment2

//import com.sun.istack.internal.logging.Logger

import ca.uwaterloo.cs.bigdata2016w.patwong.assignment2.Tokenizer
import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.rogach.scallop._

class Conf4(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

/*
  Roughly follows the structure of bespin/ComputeBigramRelativeFrequencyPairs
  Java: map -> combine -> reduce -> partition
  My Spark version is as follows:
  map -> reduceByKey -> sortByKey -> partitionBy -> map
 */
object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def staradd(s: String): (String, String) = {
    val v = (s, "*")
    return v
  }

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Use in-mapper combining: " + args.imc())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input())
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var margs1= 0.0f

    //copies the partitioner in bespin/BigramPairs
    class hadoopPart(numParts: Int) extends Partitioner {
      def numPartitions: Int = numParts
      def getPartition(key: Any): Int = {
        //need to cast key as a tuple
        val k12 = key match {
          case tuple @ (a: String, b: String) => (a, b)
        }
        return (k12._1.hashCode() & Int.MaxValue) % numPartitions
      }
    }
    val counts = textFile

      //tokenizes the list and generates tuples for pair
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val a2 = tokens.sliding(2).map(a => (a(0), a(1))).toList
          //last element in the line is dropped because bigrams cannot be calculated on last word
          val a1 = tokens.map(p => staradd(p)).toList.dropRight(1)
          a1 ::: a2   //this is how you append two lists!
                      //scala convention: last expression in a statement is the return value
        } else {
          List()
        }
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)     //(x:type, y:type) => x + y
      .sortByKey(true, args.reducers())
      .partitionBy(new hadoopPart(args.reducers()))

      //kayvee "map" analogous to the reduce job in bespin/Bigram
      .map(kayvee => {
        val k1 = kayvee._1
        if (k1._2 == "*") {
          margs1 = kayvee._2
          kayvee
        } else {
          (kayvee._1, kayvee._2.toFloat / margs1)
        }
      })
    counts.saveAsTextFile(args.output())
  }
}
