package com.iflytek.mario.tool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.event.EventBuilder;
import org.apache.flume.thrift.ThriftFlumeEvent;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.*;

public class ThriftSourceBenchTask implements Runnable {
	private static final Logger logger = LoggerFactory
			.getLogger(ThriftSourceBenchTask.class);
	private TTransport transport;
	private ThriftSourceProtocol.Client client;

	private boolean stop = false;

	ArrayList<ThriftFlumeEvent> events;
	private int maxEvents;

	public ThriftSourceBenchTask(String host, int port, int batchSize,
			int eventSize, int eventNum) throws TTransportException {
		transport = new TFramedTransport(new TSocket(host, port));
		client = new ThriftSourceProtocol.Client(
				new TCompactProtocol(transport));
		events = new ArrayList<ThriftFlumeEvent>();
		maxEvents = eventNum;
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("s.n", "flume_source_bench");
		headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
		ThriftFlumeEvent event = new ThriftFlumeEvent();
		event.setBody(new byte[eventSize]);
		event.setHeaders(headers);
		for (int i = 0; i < batchSize; ++i) {
			events.add(event);
		}
		try {
			transport.open();
		} catch (TTransportException e) {
			logger.error("Open thrift transport error:{}", e.getMessage());
			transport.close();
			throw e;
		}
	}

	@Override
	public void run() {
		int eventNum = 0;
		int errorNum = 0;
		long startTime, stopTime;

		startTime = System.currentTimeMillis();
		while (!stop) {
			// send
			try {
				if (!transport.isOpen()) {
					transport.open();
				}
				client.appendBatch(events);
				eventNum += events.size();
				if (eventNum >= maxEvents) {
					stop();
				}
			} catch (TException e1) {
				logger.error("Thread [{}] append batch error:{}", Thread
						.currentThread().getId(), e1.getMessage());
				errorNum++;
				transport.close();
			}

		}
		stopTime = System.currentTimeMillis();
		double costSec = (stopTime - startTime) / 1000.0;
		System.out.println("Thread" + Thread.currentThread().getId()
				+ " stop, time cost: " + costSec + " TPS: " + (eventNum - errorNum)
				/ costSec + " Errors: " + errorNum);
	}

	void stop() {
		stop = true;
		transport.close();
	}
}