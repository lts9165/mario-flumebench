package com.iflytek.mario.tool;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftSourceBench implements Tool {
	private static final Logger logger = LoggerFactory
			.getLogger(ThriftSourceBench.class);
	public static Options opt = new Options();
	private static String flumeIp;
	private static int flumePort;
	private static int eventSize;
	private static int threadsNum;
	private static int eventNum;
	private static int batchSize;
	private ArrayList<ThriftSourceBenchTask> allTasks;
	private ExecutorService executor;

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
				return 0;
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

		executor = Executors.newFixedThreadPool(threadsNum);
		allTasks = new ArrayList(threadsNum);
		for (int i = 0; i < threadsNum; ++i) {
			ThriftSourceBenchTask benchTask;
			try {
				benchTask = new ThriftSourceBenchTask(flumeIp, flumePort,
						batchSize, eventSize, eventNum);
				allTasks.add(benchTask);
				executor.execute(benchTask);
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		executor.shutdown();
		try {
			while (!executor.awaitTermination(5, TimeUnit.SECONDS))
				;
			executor.shutdown();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public void fini() {
		if (executor != null) {
			for (ThriftSourceBenchTask task : allTasks) {
				task.stop();
			}
			executor.shutdown();
			try {
				if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
					executor.shutdownNow();
				}
			} catch (Exception e) {
				logger.error(
						"Interrupted while waiting for transfer threads executor to shut down:{}",
						e.getMessage());
			}
		}
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