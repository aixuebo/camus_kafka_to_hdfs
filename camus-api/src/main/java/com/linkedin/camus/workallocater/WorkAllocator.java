package com.linkedin.camus.workallocater;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

//如何对待抓取的kafka数据进行分组，分到不同节点去抓取。
//因为kafka以topic+partition方式去抓取数据，因此为了并行度更高，可以让一个节点抓取多个topic+partition
public abstract class WorkAllocator {

  protected Properties props;

  public void init(Properties props){
      this.props = props;
  }

  public abstract List<InputSplit> allocateWork(List<CamusRequest> requests,
      JobContext context) throws IOException ;


}
