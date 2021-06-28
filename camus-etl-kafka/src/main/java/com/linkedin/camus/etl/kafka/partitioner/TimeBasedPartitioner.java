package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.commons.lang.LocaleUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.*;

/**
 * Partitions incoming data into time based partitions, and generates pathnames of some configured form.
 *
 * The following configurations are supported:
 * <ul>
 *     <li>{@code etl.destination.path} - top-level data output directory, required</li>
 *     <li>{@code etl.output.file.time.partition.mins} - partitions size in minutes, defaults to {@code 60}</li>
 *     <li>{@code etl.destination.path.topic.sub.dirformat} - sub-dir format to create under topic dir, defaults
 *       to {@code 'hourly'/YYYY/MM/dd/HH}. See {@link org.joda.time.format.DateTimeFormatter} for syntax.</li>
 *     <li>{@code etl.destination.path.topic.sub.dirformat.locale} - locale to use for the sub-dir formatting,
 *       defaults to {@code en_US}.
 *     <li>{@code etl.default.timezone} - timezone of the events, defaults to {@code America/Los_Angeles}</li>
 * </ul>
 * 分钟级别的分区，需要设置分区的目录名字，以及分钟间隔
 */
public class TimeBasedPartitioner extends BaseTimeBasedPartitioner {

  //默认是60分钟一个间隔，因此相当于一小时，因此默认是以下2个配置，如果不是60分钟，则需要重新定义这两个配置
  private static final String DEFAULT_TOPIC_SUB_DIR_FORMAT = "'hourly'/YYYY/MM/dd/HH";
  private static final String DEFAULT_PARTITION_DURATION_MINUTES = "60";

  private static final String DEFAULT_LOCALE = Locale.US.toString();

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {

      String destPathTopicSubDirFormat = conf.get(ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT, DEFAULT_TOPIC_SUB_DIR_FORMAT);
      long partitionDurationMinutes = Long.parseLong(conf.get(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, DEFAULT_PARTITION_DURATION_MINUTES));

      Locale locale = LocaleUtils.toLocale(conf.get(ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT_LOCALE, DEFAULT_LOCALE));
      DateTimeZone outputTimeZone = DateTimeZone.forID(conf.get(ETL_DEFAULT_TIMEZONE, DEFAULT_TIME_ZONE));

      long outfilePartitionMs = TimeUnit.MINUTES.toMillis(partitionDurationMinutes);//根据分钟级别间隔，设置分区时间间隔

      init(outfilePartitionMs, destPathTopicSubDirFormat, locale, outputTimeZone);
    }

    super.setConf(conf);
  }

}
