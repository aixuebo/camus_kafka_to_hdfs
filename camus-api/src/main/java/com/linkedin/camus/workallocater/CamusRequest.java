package com.linkedin.camus.workallocater;

import java.net.URI;

import org.apache.hadoop.io.Writable;

//代表一个mapper要去抓取某一个topic+partition上的数据信息
//包含topic名字、partition序号、partition的leader地址、leader的唯一ID、目前kafka上最早的offset和最晚的offset信息、从什么offset开始读取数据
public interface CamusRequest extends Writable {

  public abstract void setLatestOffset(long latestOffset);

  public abstract void setEarliestOffset(long earliestOffset);

  /**
   * Sets the starting offset used by the kafka pull mapper.
   * 
   * @param offset
   */
  public abstract void setOffset(long offset);

  /**
   * Sets the broker uri for this request
   * 
   * @param uri
   * partition的leader节点地址
   */
  public abstract void setURI(URI uri);

  /**
   * Retrieve the topic
   * 
   * @return
   * topic名字
   */
  public abstract String getTopic();

  /**
   * Retrieves the uri if set. The default is null.
   * 
   * @return
   */
  public abstract URI getURI();

  /**
   * Retrieves the partition number
   * 
   * @return
   */
  public abstract int getPartition();

  /**
   * Retrieves the offset
   * 
   * @return
   */
  public abstract long getOffset();

  /**
   * Returns true if the offset is valid (>= to earliest offset && <= to last
   * offset)
   * 
   * @return
   */
  public abstract boolean isValidOffset();

  public abstract long getEarliestOffset();

  public abstract long getLastOffset();

  public abstract long getLastOffset(long time);

  public abstract long estimateDataSize();
  
  public abstract void setAvgMsgSize(long size);

  /**
   * Estimates the request size in bytes by connecting to the broker and
   * querying for the offset that bets matches the endTime.
   * 
   * @param endTime
   *            The time in millisec
   */
  public abstract long estimateDataSize(long endTime);

}
