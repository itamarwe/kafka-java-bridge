package com.liveperson.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.net.SyslogAppender;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by itamarwe on 1/9/2017.
 */
public class TopicFilterHLConsumer {

    private final static int TERMINATION_SECS = 5;

    private ExecutorService executor;
    private ConsumerConnector consumer;
    private String topic;
    private ConsumerThread consumerThread;
    private int threadCount;
    private boolean debugPrintStats;
    private ThreadExceptionListener exceptionListener;
    private long debugPrintIntervalMS;
    private int consumerServerPort;
    private Properties properties;
    private AtomicBoolean isPaused;
    private TopicFilter topicFilter;

    public TopicFilterHLConsumer(String zookeeper, String groupId, String topicPattern, Properties clientProps, int consumerServerPort, int threadCount, ThreadExceptionListener exceptionListener){
        properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);
        properties.put("group.id", groupId);
        if(clientProps != null){
            properties.putAll(clientProps);
        }

        this.topicFilter = new Whitelist(topicPattern);
        this.threadCount = threadCount;
        this.exceptionListener = exceptionListener;
        this.consumerServerPort = consumerServerPort;
        this.isPaused = new AtomicBoolean(false);
    }

    public void start(){
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        List<KafkaStream<byte[], byte[]>> streams  = consumer.createMessageStreamsByFilter(topicFilter, threadCount);

        executor = Executors.newFixedThreadPool(threadCount);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerThread(stream, threadNumber, consumerServerPort, exceptionListener, isPaused));
            threadNumber++;
        }
    }

    public void stop(){
        if (consumer != null) {
            consumer.shutdown();
            resume();
        }

        if (executor != null) {
            executor.shutdown();
            try {
                if(!executor.awaitTermination(TERMINATION_SECS, TimeUnit.SECONDS)){
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    public void pause(){
      synchronized (isPaused) {
        isPaused.set(true);
        isPaused.notifyAll();
      }
    }

    public void resume(){
      synchronized (isPaused) {
        isPaused.set(false);
        isPaused.notifyAll();
      }
    }
}
