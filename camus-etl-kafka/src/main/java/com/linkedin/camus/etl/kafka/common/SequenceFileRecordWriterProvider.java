package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;


/**
 * Provides a RecordWriter that uses SequenceFile.Writer to write
 * SequenceFiles records to HDFS.  Compression settings are controlled via
 * the usual hadoop configuration values.
 *
 * - mapreduce.output.fileoutputformat.compress         - true or false
 * - mapreduce.output.fileoutputformat.compress.codec   - org.apache.hadoop.io.compress.* (SnappyCodec, etc.)
 * - mapreduce.output.fileoutputformat.compress.type    - BLOCK or RECORD
 *
 * 进入该类的kafka的value一定也是String类型的json信息。
 * 最终每一条数据存储的格式为:writer.append(new LongWritable(key.getTime()), new Text(record));
 * 即事件时间戳 以及 事件内容
 */
public class SequenceFileRecordWriterProvider implements RecordWriterProvider {

  private static Logger log = Logger.getLogger(SequenceFileRecordWriterProvider.class);

  public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
  public static final String DEFAULT_RECORD_DELIMITER = "";
  protected String recordDelimiter = null; //行之间的分割方式，该类处理的不是字符串类型,因此有特殊的分割方式，不需要单独定义分割方式

  public SequenceFileRecordWriterProvider(TaskAttemptContext context) {

  }

  // TODO: Make this configurable somehow.
  // To do this, we'd have to make SequenceFileRecordWriterProvider have an
  // init(JobContext context) method signature that EtlMultiOutputFormat would always call.
  @Override
  public String getFilenameExtension() {
    return "";
  }

  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
      CamusWrapper camusWrapper, FileOutputCommitter committer) throws IOException, InterruptedException {

    Configuration conf = context.getConfiguration();

    // If recordDelimiter hasn't been initialized, do so now
    // 行之间的分割方式，该类处理的不是字符串类型,因此有特殊的分割方式，不需要单独定义分割方式
    if (recordDelimiter == null) {
      recordDelimiter = conf.get(ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
    }

    CompressionCodec compressionCodec = null;
    CompressionType compressionType = CompressionType.NONE;

    // Determine compression type (BLOCK or RECORD) and compression codec to use.
    if (SequenceFileOutputFormat.getCompressOutput(context)) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);
      Class<?> codecClass = SequenceFileOutputFormat.getOutputCompressorClass(context, DefaultCodec.class);
      // Instantiate the CompressionCodec Class
      compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    // Get the filename for this RecordWriter.
    Path path =
        new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

    log.info("Creating new SequenceFile.Writer with compression type " + compressionType + " and compression codec "
        + (compressionCodec != null ? compressionCodec.getClass().getName() : "null"));
    final SequenceFile.Writer writer =
        SequenceFile.createWriter(path.getFileSystem(conf), conf, path, LongWritable.class, Text.class,
            compressionType, compressionCodec, context);

    // Return a new anonymous RecordWriter that uses the
    // SequenceFile.Writer to write data to HDFS
    return new RecordWriter<IEtlKey, CamusWrapper>() {
      @Override
      public void write(IEtlKey key, CamusWrapper data) throws IOException, InterruptedException {
        String record = (String) data.getRecord() + recordDelimiter;
        // Use the timestamp from the EtlKey as the key for this record.
        // TODO: Is there a better key to use here?
        writer.append(new LongWritable(key.getTime()), new Text(record));
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }
}
