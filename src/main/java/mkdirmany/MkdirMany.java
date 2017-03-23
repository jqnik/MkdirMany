
package mkdirmany;

import java.io.IOException;
import java.lang.Integer;
import java.io.File;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import java.util.UUID;

class MkdirThread implements Runnable {

  private int nrIters = 10;
  private String base = "/user/ec2-user/MkdirsMany-"; 

  public MkdirThread(int nrIters, String base) {
    this.nrIters = nrIters;
    this.base = base;

  }

	static void threadMessage(String message) {
    String threadName =
    Thread.currentThread().getName();
    System.out.format("%s: %s%n", threadName, message);
	}

  public void run() {
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.setQuietMode(false);
    //conf.set("fs.defaultFS", "hdfs://ip-10-0-0-209.us-west-2.compute.internal:8020");
    try {
      FileSystem fs = FileSystem.get(conf);
      String path = base += UUID.randomUUID().toString();
      for (int i = 0; i < nrIters; i++) {
        String finalPath = path + i;
        fs.mkdirs(new Path(finalPath));
        fs.delete(new Path(finalPath), true);
      }
    } catch (java.io.IOException e) {
      System.err.println("Error occured during HDFS I/O");
			System.exit(1);
    }
  }
}

public class MkdirMany {

  static private int nrThreads = 30;
  static private int nrIters = 4000;
  static String base = "/user/ec2-user/MkdirsMany-";
  static private Configuration conf;





  public static void main(String argv[]) throws Exception {


    Options options = new Options();

    Option oThreads = new Option("t", "threads", true, "Number of threads");
    oThreads.setRequired(true);
    options.addOption(oThreads);

    Option oIters = new Option("i", "iterations", true, "Number of Iterataions in each thread");
    oIters.setRequired(true);
    options.addOption(oIters);

    Option oBase = new Option("b", "basedir", true, "Base directory in HDFS to create directories");
    oBase.setRequired(true);
    options.addOption(oBase);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
        cmd = parser.parse(options, argv);
    } catch (ParseException e) {
        System.out.println(e.getMessage());
        formatter.printHelp("MkdirMany", options);

        System.exit(1);
        return;
    }

    nrThreads = Integer.parseInt(cmd.getOptionValue("threads"));
    nrIters = Integer.parseInt(cmd.getOptionValue("iterations"));
    base = cmd.getOptionValue("basedir");

    if(base.charAt(base.length()-1)!=File.separatorChar){
        base += File.separator;
    }

		System.out.println("Starting " + nrThreads + " threads with " + nrIters + " iterations each");
		long startTime = System.currentTimeMillis();
		Thread myThreads[] = new Thread[nrThreads];
		for (int j = 0; j < nrThreads; j++) {
				myThreads[j] = new Thread(new MkdirThread(nrIters, base));
				myThreads[j].start();
		}
		System.out.println("Waiting for threads to finish");
		for (int j = 0; j < nrThreads; j++) {
				myThreads[j].join(); //todo add catch exception
		}

    System.exit(0);

  }


}
