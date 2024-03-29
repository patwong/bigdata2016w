package ca.uwaterloo.cs.bigdata2016w.patwong.assignment2

import ca.uwaterloo.cs.bigdata2016w.patwong.assignment2.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf3(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

object brtest extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf3 = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf3)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val pairs = textFile.map(s => (s, 1))
    val counts = pairs.reduceByKey(_ + _)
    counts.saveAsTextFile(args.output())
/*    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args.output())
*/
  }
}
