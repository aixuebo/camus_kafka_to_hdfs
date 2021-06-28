package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;


/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 *
 * @author Richard Park
 * 连接topic+partition的leader节点,创造kafka的消费者，去获取某一个topic+partition的数据信息，
 * 已知要抓取的offset范围是[beginOffset,lastOffset],currentOffset用于跟踪已经抓取到的offset位置
 *
 * 目标，读取一个topic+partition数据，返回EtlKey和KafkaMessage
 */
public class KafkaReader {
  // index of context
  private static Logger log = Logger.getLogger(KafkaReader.class);
  private EtlRequest kafkaRequest = null;//代表一个mapper要去抓取某一个topic+partition上的数据信息
  private SimpleConsumer simpleConsumer = null;//kafka消费者

  private long beginOffset;//开始从哪个offset开始抓取
  private long currentOffset;//下一个要抓取的offset
  private long lastOffset;//抓取到哪里结束
  private long currentCount;//当前抓取了多少条数据

  private TaskAttemptContext context;

  private Iterator<MessageAndOffset> messageIter = null;//记录每一个offset和对应的message信息,message中包含了key和value

  private long totalFetchTime = 0;//抓取请求到收到数据的总耗时
  private long lastFetchTime = 0;//上一次抓取请求到收到数据的耗时

  private int fetchBufferSize;//每次抓取多少条数据回来


  /**
   * Construct using the json representation of the kafka request
   */
  public KafkaReader(EtlInputFormat inputFormat, TaskAttemptContext context, EtlRequest request, int clientTimeout,
      int fetchBufferSize) throws Exception {
    this.fetchBufferSize = fetchBufferSize;
    this.context = context;

    log.info("bufferSize=" + fetchBufferSize);
    log.info("timeout=" + clientTimeout);

    // Create the kafka request from the json

    kafkaRequest = request;

    beginOffset = request.getOffset();
    currentOffset = request.getOffset();
    lastOffset = request.getLastOffset();
    currentCount = 0;
    totalFetchTime = 0;

    // read data from queue

    URI uri = kafkaRequest.getURI();//topic+partition的leader节点
    simpleConsumer = inputFormat.createSimpleConsumer(context, uri.getHost(), uri.getPort());
    log.info("Connected to leader " + uri + " beginning reading at offset " + beginOffset + " latest offset="
        + lastOffset);
    fetch();
  }

  public boolean hasNext() throws IOException {
    if (currentOffset >= lastOffset) {
      return false;
    }
    if (messageIter != null && messageIter.hasNext()) {
      return true;
    } else {
      return fetch();
    }
  }

  /**
   * Fetches the next Kafka message and stuffs the results into the key and
   * value
   *
   * @param etlKey
   * @return true if there exists more events
   * @throws IOException
   */
  public KafkaMessage getNext(EtlKey etlKey) throws IOException {
    if (hasNext()) {

      MessageAndOffset msgAndOffset = messageIter.next();
      Message message = msgAndOffset.message();

      byte[] payload = getBytes(message.payload());
      byte[] key = getBytes(message.key());

      if (payload == null) {
        log.warn("Received message with null message.payload(): " + msgAndOffset);
      }

      etlKey.clear();
      etlKey.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(), currentOffset,
          msgAndOffset.offset() + 1, message.checksum());

      etlKey.setMessageSize(msgAndOffset.message().size());

      currentOffset = msgAndOffset.offset() + 1; // increase offset
      currentCount++; // increase count

      return new KafkaMessage(payload, key, kafkaRequest.getTopic(), kafkaRequest.getPartition(),
          msgAndOffset.offset(), message.checksum());
    } else {
      return null;
    }
  }

  private byte[] getBytes(ByteBuffer buf) {
    byte[] bytes = null;
    if (buf != null) {
      int size = buf.remaining();
      bytes = new byte[size];
      buf.get(bytes, buf.position(), size);
    }
    return bytes;
  }

  /**
   * Creates a fetch request.
   *
   * @return false if there's no more fetches
   * @throws IOException
   * 请求kafka,抓取topic+partition的数据
   */
  public boolean fetch() throws IOException {
    if (currentOffset >= lastOffset) {
      return false;
    }
    long tempTime = System.currentTimeMillis();
    TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaRequest.getTopic(), kafkaRequest.getPartition());//抓取哪个topic+partition
    log.debug("\nAsking for offset : " + (currentOffset));
    PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(currentOffset, fetchBufferSize);//从哪个offset开始抓取，期待抓取多少个回来

    HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
    fetchInfo.put(topicAndPartition, partitionFetchInfo);

    //发送抓取请求，抓取topic+partition数据，从offset开始抓取，抓取多少条
    FetchRequest fetchRequest =
        new FetchRequest(CamusJob.getKafkaFetchRequestCorrelationId(context), CamusJob.getKafkaClientName(context),
            CamusJob.getKafkaFetchRequestMaxWait(context), CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

    FetchResponse fetchResponse = null;
    try {
      fetchResponse = simpleConsumer.fetch(fetchRequest);
      if (fetchResponse.hasError()) {
        String message =
            "Error Code generated : " + fetchResponse.errorCode(kafkaRequest.getTopic(), kafkaRequest.getPartition())
                + "\n";
        throw new RuntimeException(message);
      }
      return processFetchResponse(fetchResponse, tempTime);//抓取的请求前时间戳
    } catch (Exception e) {
      log.info("Exception generated during fetch for topic " + kafkaRequest.getTopic() + ": " + e.getMessage()
          + ". Will refresh topic metadata and retry.");
      return refreshTopicMetadataAndRetryFetch(fetchRequest, tempTime);
    }
  }

  //刷新元数据，并且抓取kafka的消息
  private boolean refreshTopicMetadataAndRetryFetch(FetchRequest fetchRequest, long tempTime) {
    try {
      refreshTopicMetadata();
      FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
      if (fetchResponse.hasError()) {
        log.warn("Error encountered during fetch request retry from Kafka");
        log.warn("Error Code generated : "
            + fetchResponse.errorCode(kafkaRequest.getTopic(), kafkaRequest.getPartition()));
        return false;
      }
      return processFetchResponse(fetchResponse, tempTime);
    } catch (Exception e) {
      log.info("Exception generated during fetch for topic " + kafkaRequest.getTopic()
          + ". This topic will be skipped.");
      return false;
    }
  }

  //刷新元数据 ,请求topic+partition最新的leader节点
  private void refreshTopicMetadata() {
    TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(kafkaRequest.getTopic()));
    TopicMetadataResponse response;
    try {
      response = simpleConsumer.send(request);
    } catch (Exception e) {
      log.error("Exception caught when refreshing metadata for topic " + request.topics().get(0) + ": "
          + e.getMessage());
      return;
    }
    TopicMetadata metadata = response.topicsMetadata().get(0);
    for (PartitionMetadata partitionMetadata : metadata.partitionsMetadata()) {
      if (partitionMetadata.partitionId() == kafkaRequest.getPartition()) {
        simpleConsumer =
            new SimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(),
                CamusJob.getKafkaTimeoutValue(context), CamusJob.getKafkaBufferSize(context),
                CamusJob.getKafkaClientName(context));
        break;
      }
    }
  }

  //抓取的数据结果，以及抓取前的时间戳，用于计算抓取的耗时
  private boolean processFetchResponse(FetchResponse fetchResponse, long tempTime) {
    try {
      ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(kafkaRequest.getTopic(), kafkaRequest.getPartition());
      lastFetchTime = (System.currentTimeMillis() - tempTime);
      log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
      log.debug("The size of the ByteBufferMessageSet returned is : " + messageBuffer.sizeInBytes());
      int skipped = 0;
      totalFetchTime += lastFetchTime;
      messageIter = messageBuffer.iterator();
      //boolean flag = false;
      Iterator<MessageAndOffset> messageIter2 = messageBuffer.iterator();
      MessageAndOffset message = null;
      while (messageIter2.hasNext()) {
        message = messageIter2.next();
        if (message.offset() < currentOffset) {//跳过无效的offset
          //flag = true;
          skipped++;
        } else {
          log.debug("Skipped offsets till : " + message.offset());
          break;
        }
      }
      log.debug("Number of offsets to be skipped: " + skipped);
      while (skipped != 0) {
        MessageAndOffset skippedMessage = messageIter.next();
        log.debug("Skipping offset : " + skippedMessage.offset());
        skipped--;
      }

      if (!messageIter.hasNext()) {
        System.out.println("No more data left to process. Returning false");
        messageIter = null;
        return false;
      }

      return true;
    } catch (Exception e) {
      log.info("Exception generated during processing fetchResponse");
      return false;
    }
  }

  /**
   * Closes this context
   *
   * @throws IOException
   */
  public void close() throws IOException {
    if (simpleConsumer != null) {
      simpleConsumer.close();
    }
  }

  /**
   * Returns the total bytes that will be fetched. This is calculated by
   * taking the diffs of the offsets
   *
   * @return
   * 总共需要读取多少条数据
   */
  public long getTotalBytes() {
    return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
  }

  /**
   * Returns the total bytes that have been fetched so far
   *
   * @return
   * 已经读取多少条数据
   */
  public long getReadBytes() {
    return currentOffset - beginOffset;
  }

  /**
   * Returns the number of events that have been read r
   *
   * @return
   */
  public long getCount() {
    return currentCount;
  }

  /**
   * Returns the fetch time of the last fetch in ms
   *
   * @return
   */
  public long getFetchTime() {
    return lastFetchTime;
  }

  /**
   * Returns the totalFetchTime in ms
   *
   * @return
   */
  public long getTotalFetchTime() {
    return totalFetchTime;
  }
}
