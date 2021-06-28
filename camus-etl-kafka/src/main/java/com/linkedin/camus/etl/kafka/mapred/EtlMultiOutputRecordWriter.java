package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;


public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object> {
  private TaskAttemptContext context;
  private Writer errorWriter = null;
  private String currentTopic = "";
  private long beginTimeStamp = 0;//当前系统时间戳 - 该值,如果kafka的事件时间戳<该结果,数据要被丢弃，说明是处理的老数据，不需要再处理了
  private static Logger log = Logger.getLogger(EtlMultiOutputRecordWriter.class);
  private final Counter topicSkipOldCounter;

  //每一个数据写入的分区是不同的，因此缓存每一个文件对应一个输出流
  //key是文件名、value是对应的输出流
  private HashMap<String, RecordWriter<IEtlKey, CamusWrapper>> dataWriters = new HashMap<String, RecordWriter<IEtlKey, CamusWrapper>>();


  private EtlMultiOutputCommitter committer;

  public EtlMultiOutputRecordWriter(TaskAttemptContext context, EtlMultiOutputCommitter committer) throws IOException,
      InterruptedException {
    this.context = context;
    this.committer = committer;
    errorWriter =
        SequenceFile.createWriter(
            FileSystem.get(context.getConfiguration()),
            context.getConfiguration(),
            new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context,
                EtlMultiOutputFormat.ERRORS_PREFIX, "")), EtlKey.class, ExceptionWritable.class);

    if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
      int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
      beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
    } else {
      beginTimeStamp = 0;
    }
    log.info("beginTimeStamp set to: " + beginTimeStamp);
    topicSkipOldCounter = getTopicSkipOldCounter();
  }

  private Counter getTopicSkipOldCounter() {
    try {
      //In Hadoop 2, TaskAttemptContext.getCounter() is available
      Method getCounterMethod = context.getClass().getMethod("getCounter", String.class, String.class);
      return ((Counter) getCounterMethod.invoke(context, "total", "skip-old"));
    } catch (NoSuchMethodException e) {
      //In Hadoop 1, TaskAttemptContext.getCounter() is not available
      //Have to cast context to TaskAttemptContext in the mapred package, then get a StatusReporter instance
      org.apache.hadoop.mapred.TaskAttemptContext mapredContext = (org.apache.hadoop.mapred.TaskAttemptContext) context;
      return ((StatusReporter) mapredContext.getProgressible()).getCounter("total", "skip-old");
    } catch (IllegalArgumentException e) {
      log.error("IllegalArgumentException while obtaining counter 'total:skip-old': " + e.getMessage());
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      log.error("IllegalAccessException while obtaining counter 'total:skip-old': " + e.getMessage());
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      log.error("InvocationTargetException obtaining counter 'total:skip-old': " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    for (String w : dataWriters.keySet()) {
      dataWriters.get(w).close(context);
    }
    errorWriter.close();
  }

  /**
   1.每次在输出一个数据时,相当于该数据已经被消费了，因此要提交offset。
   2.每一个数据写入的分区是不同的，因此缓存每一个文件对应一个输出流
   **/
  @Override
  public void write(EtlKey key, Object val) throws IOException, InterruptedException {
    if (val instanceof CamusWrapper<?>) {
      if (key.getTime() < beginTimeStamp) {//说明数据内容是好几天前的了，要抛弃掉
        //TODO: fix this logging message, should be logged once as a total count of old records skipped for each topic
        // for now, commenting this out
        //log.warn("Key's time: " + key + " is less than beginTime: " + beginTimeStamp);
        topicSkipOldCounter.increment(1);
        committer.addOffset(key);
      } else {
        if (!key.getTopic().equals(currentTopic)) {
          for (RecordWriter<IEtlKey, CamusWrapper> writer : dataWriters.values()) {
            writer.close(context);
          }
          dataWriters.clear();
          currentTopic = key.getTopic();
        }

        committer.addCounts(key);
        CamusWrapper value = (CamusWrapper) val;
        String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);
        if (!dataWriters.containsKey(workingFileName)) {
          dataWriters.put(workingFileName, getDataRecordWriter(context, workingFileName, value));
          log.info("Writing to data file: " + workingFileName);
        }
        dataWriters.get(workingFileName).write(key, value);
      }
    } else if (val instanceof ExceptionWritable) {
      committer.addOffset(key);
      log.warn("ExceptionWritable key: " + key + " value: " + val);
      errorWriter.append(key, (ExceptionWritable) val);
    } else {
      log.warn("Unknow type of record: " + val);
    }
  }

  private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
      CamusWrapper value) throws IOException, InterruptedException {
    RecordWriterProvider recordWriterProvider = null;
    try {
      //recordWriterProvider = EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
      Class<RecordWriterProvider> rwp = EtlMultiOutputFormat.getRecordWriterProviderClass(context);
      Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
      recordWriterProvider = crwp.newInstance(context);
    } catch (InstantiationException e) {
      throw new IllegalStateException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return recordWriterProvider.getDataRecordWriter(context, fileName, value, committer);
  }
}
