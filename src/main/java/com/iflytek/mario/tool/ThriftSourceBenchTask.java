package com.iflytek.mario.tool;

import java.util.ArrayList;

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
      .getLogger(ThriftSourceBench.class);
  private TTransport transport;
  private ThriftSourceProtocol.Client client;
  
  boolean sleep = false;
  boolean stop = false;
  
  ArrayList<ThriftFlumeEvent> events;
  
  public ThriftSourceBenchTask(String host, int port, int batchSize,
      int eventSize, int eventNum) throws TTransportException {
    transport = new TFramedTransport(new TSocket(host, port));
    client = new ThriftSourceProtocol.Client(new TCompactProtocol(transport));
    events = new ArrayList<ThriftFlumeEvent>();
    ThriftFlumeEvent event = new ThriftFlumeEvent();
    
    for (int i = 0; i < batchSize; ++i) {
      
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
    while (!stop) {
      
      // fetch log from queue.
      ThriftFlumeEvent event = null;
      if (null != event) {
        events.add(event);
      }
      
      // send
      try {
        if (!transport.isOpen()) {
          transport.open();
        }
        if (events.size() > 0) {
          client.appendBatch(events);
          events.clear();
        }
        
      } catch (TException e1) {
        e1.printStackTrace();
        logger.error("Thread [{}] append batch error:{}", Thread
            .currentThread().getId(), e1.getMessage());
        transport.close();
      }
      
    }
  }
  
  private void stop() {
    stop = true;
    transport.close();
  }
}