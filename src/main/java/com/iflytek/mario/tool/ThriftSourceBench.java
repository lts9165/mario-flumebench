package com.iflytek.mario.tool;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ThriftSourceBench implements Tool {
  public static Options opt = new Options();
  private static String flumeIp;
  private static int flumePort;
  private static int eventSize;
  private static int threadsNum;
  private static int eventNum;
  private static int batchSize;
  public ThriftSourceBench() {
    opt.addOption("h", "help", false, "Print help for this bench tool");
    opt.addOption("t", "threads", true, "The num of bench threads");
    opt.addOption("s", "size", true, "The data size of event");
    opt.addOption("n", "num", true, "The num of events each thread send");
    opt.addOption("i", "ip", true, "Flume ip");
    opt.addOption("p", "port", true, "Flume port");
    opt.addOption("b", "batchsize", true, "Batch Size");
  }
  
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) {
    try {
      BasicParser parser = new BasicParser();
      String[] argss = new String[args.size()];
      CommandLine cl = parser.parse(opt, args.toArray(argss));
      if (cl.hasOption('h')) {
        HelpFormatter f = new HelpFormatter();
        f.printHelp("thriftsource", opt);
      } else {
        flumeIp = cl.getOptionValue("i");
        flumePort = Integer.parseInt(cl.getOptionValue("p"));
        eventSize = Integer.parseInt(cl.getOptionValue("s"));
        threadsNum = Integer.parseInt(cl.getOptionValue("t"));
        eventNum = Integer.parseInt(cl.getOptionValue("n"));
        if (cl.getOptionValue("b") == null)
          batchSize = 1000;
        else
          batchSize = Integer.parseInt(cl.getOptionValue("b"));
      }
    } catch (ParseException e) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("thriftsource", opt);
      return 0;
    }
  
    
    return 0;
  }
  
  @Override
  public String getName() {
    return "thriftsource";
  }
  
  @Override
  public String getShortDescription() {
    return "flume thrift source bench";
  }
  
}