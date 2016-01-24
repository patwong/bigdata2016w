package ca.uwaterloo.cs.bigdata2016w.patwong.assignment2

//import com.sun.istack.internal.logging.Logger

import ca.uwaterloo.cs.bigdata2016w.patwong.assignment2.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf4(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def wcIter(iter: Iterator[String]): Iterator[(String, Int)] = {
    val counts = new HashMap[String, Int]() { override def default(key: String) = 0 }

    iter.flatMap(line => tokenize(line))
      .foreach { t => counts.put(t, counts(t) + 1) }

    counts.iterator
  }
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


    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val a1 = tokens.map(p => staradd(p)).toList
          val a2 = tokens.sliding(2).map(a => (a(0), a(1))).toList
          a1 ::: a2
        } else {
          List()
        }
      })
      .map(bigram => (bigram, 1))
      .sortByKey(true, args.reducers())
      .reduceByKey(_ + _)
  //    .map(kayvee => {
//        if
      //})



    counts.saveAsTextFile(args.output())
  }
}
