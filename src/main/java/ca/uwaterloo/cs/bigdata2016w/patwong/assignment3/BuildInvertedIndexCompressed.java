package ca.uwaterloo.cs.bigdata2016w.patwong.assignment3;
//package io.bespin.java.mapreduce.search;

import java.io.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableUtils;  //added for index compression
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringInt;


public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
//    private static final Text WORD = new Text();
    private static final PairOfStringInt II_mapKey = new PairOfStringInt();
    private static final IntWritable II_mapVal = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      // Tokenize line.
      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(text);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        //WORD.set(e.getLeftElement());
        II_mapKey.set(e.getLeftElement(), (int) docno.get());
        II_mapVal.set(e.getRightElement());
        context.write(II_mapKey, II_mapVal);
      }
    }
  }
  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, PairOfWritables<ArrayListWritable<VIntWritable>,ArrayListWritable<VIntWritable>>> {
    private final static IntWritable DF = new IntWritable();
    private static final Text WORD = new Text();
    private static String t_prev = "";
    private static int df;
    private static ArrayListWritable<VIntWritable> Arr1 = new ArrayListWritable<VIntWritable>();
    private static ArrayListWritable<VIntWritable> Arr2 = new ArrayListWritable<VIntWritable>();

    //FileOutputStream fop1 = NULL;
    //FileOutputStream fop2 = NULL;
    //FileInputStream fin1 = NULL;
    //FileInputStream fin2 = NULL;
    private static File file_n;
    private static File file_f;
    private static FileOutputStream fop1;
    private static FileOutputStream fop2;
    private static FileInputStream fin1;
    private static FileInputStream fin2;
    private static DataOutput out1;
    private static DataOutput out2;
    private static DataInput in1;
    private static DataInput in2;
    private static int dgap;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
      //initialize arrays
//      ArrayListWritable<VInt> vint_n = new ArrayListWritable<VIntWritable>();
//      ArrayListWritable<VInt> vint_f = new ArrayListWritable<VIntWritable>();
      file_n = new File("buffer1.txt");
      file_f = new File("buffer2.txt");
      fop1 = new FileOutputStream(file_n);
      fop2 = new FileOutputStream(file_f);
      fin1 = new FileInputStream(file_n);
      fin2 = new FileInputStream(file_f);
      out1 = new DataOutputStream(fop1);
      out2 = new DataOutputStream(fop2);
      in1 = new DataInputStream(fin1);
      in2 = new DataInputStream(fin2);
      df = 0;
      dgap = 0;
    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      Iterator<IntWritable> iter = values.iterator();

      if(!(t_prev.equals(key.getLeftElement())) && !(t_prev.equals("")) ) {
        fop1.flush();
        fop2.flush();

        int j1 = 0, j2 = 0;
        for(int c = 0; c < df; c++) {
          j1 = WritableUtils.readVInt(in1);
          j2 = WritableUtils.readVInt(in2);
          Arr1.add(new VIntWritable(j1));
          Arr2.add(new VIntWritable(j2));
        }
        PrintWriter writer1 = new PrintWriter(file_n);
        PrintWriter writer2 = new PrintWriter(file_f);
        writer1.print("");
        writer1.close();
        writer2.print("");
        writer2.close();
        WORD.set(key.getLeftElement());
        context.write(WORD, new PairOfWritables<ArrayListWritable<VIntWritable>, ArrayListWritable<VIntWritable>>(Arr1, Arr2));
        df = 0;
        Arr1.clear();
        Arr2.clear();
      }

      while (iter.hasNext()) {
        df++;
        WritableUtils.writeVInt(out1,key.getRightElement()-dgap);  //getting n
        WritableUtils.writeVInt(out2, iter.next().get());               //getting f
      }
      dgap = key.getRightElement();
      t_prev = key.getLeftElement();
    }
    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      fop1.flush();
      fop2.flush();
      int j1 = 0, j2 = 0;
      j1 = WritableUtils.readVInt(in1);
      j2 = WritableUtils.readVInt(in2);
      Arr1.add(new VIntWritable(j1));
      Arr2.add(new VIntWritable(j2));
      PrintWriter writer1 = new PrintWriter(file_n);
      PrintWriter writer2 = new PrintWriter(file_f);
      writer1.print("");
      writer1.close();
      writer2.print("");
      writer2.close();
      WORD.set(t_prev);
      context.write(WORD, new PairOfWritables<ArrayListWritable<VIntWritable>, ArrayListWritable<VIntWritable>>(Arr1, Arr2));
    }
  }

  protected static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
      //return key.getLeftElement().hashCode() % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

//    job.setNumReduceTasks(1);
    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
