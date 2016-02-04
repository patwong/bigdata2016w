package ca.uwaterloo.cs.bigdata2016w.patwong.assignment3;
//package io.bespin.java.mapreduce.search;

import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import java.util.Iterator;


import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;



public class BooleanRetrievalCompressed extends Configured implements Tool {
  private MapFile.Reader[] index;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;

  private BooleanRetrievalCompressed() {}
  private static int foldercount = 0;

  //to count how many folders are created
  //obtained from internet:
  //http://stackoverflow.com/questions/1844688/read-all-files-in-a-folder
  public void listFilesForFolder(final File folder) {
    for (final File fileEntry : folder.listFiles()) {
      if (fileEntry.isDirectory()) {
        foldercount++;
      }
    }
 //   System.out.println("The folder count is " + foldercount + "."); //debug
  }
  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {

    listFilesForFolder(new File(indexPath));
 //   System.out.println("The folder count is " + foldercount + "."); //debug

    index = new MapFile.Reader[foldercount];
    for(int i = 0; i < foldercount; i++) {
      if(i > 9) { //double digit folder index format
        index[i] = new MapFile.Reader(new Path(indexPath + "/part-r-000" + i), fs.getConf());
      } else {    //single digit folder index format
        index[i] = new MapFile.Reader(new Path(indexPath + "/part-r-0000" + i), fs.getConf());
        if(index[i] == null)
          System.out.println("index[" + i + "] IS NULL!!!!!!");
      }
    }
    //System.out.println("size of INDEX is: " + index.size());
    collection = fs.open(new Path(collectionPath));
    stack = new Stack<Set<Integer>>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }
    //if(stack.empty()) {
//      System.out.println("runQuery: SET is EMPTY");
//    } else {
//      System.out.println("runQuery: SET is NOT EMPTY!!!!");
//    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println("for loop in runquery:" + i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    System.out.println("pushTerm returns:"+fetchDocumentSet(term));
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }
    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<Integer>();
    //fetchDocumentSet expects a VIntWritable - an array is returned here
//    if(fetchPostings(term) == null) {
//      System.out.println("IT IS NULL SOMETHING IS WRONG");
//    } else {
//      System.out.println("IT IS _NOT_ NULL SOMETHING IS NOT WRONG?");
//    }
    for (VIntWritable x: fetchPostings(term)) {
      //System.out.println("this is in the vintwritable:" + x.get());
      set.add(x.get());
    }
    if(set.isEmpty()) {
      System.out.println("the set is empty!!!!");
    } else {
      System.out.println("the set is NOT empty");
      Iterator iter = set.iterator();
      while(iter.hasNext()) {
        System.out.println(iter.next());
      }
    }
    return set;
  }

  private ArrayListWritable<VIntWritable> fetchPostings(String term) throws IOException {
    Text key = new Text();
    PairOfWritables<ArrayListWritable<VIntWritable>,ArrayListWritable<VIntWritable>>   value =
        new PairOfWritables<ArrayListWritable<VIntWritable>,ArrayListWritable<VIntWritable>>();
    key.set(term);
    int z = (term.hashCode() & Integer.MAX_VALUE) % (foldercount);
    index[z].get(key, value);
    if(value == null) {
      System.out.println("VALUE IS NULL!!!!");
    }
    if(value.getLeftElement() == null) {
      System.out.println("LEFT VALUE IS NULL!!!!");
    }
    if(value.getRightElement() == null) {
      System.out.println("RIGHT VALUE IS NULL!!!!");
    }
//    System.out.println("term is: " + term);
//    System.out.println("index is: " + z);
    return value.getLeftElement();
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  public static class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    public String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    public String query;
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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
