package com.linkedin.camus.etl.kafka.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.workallocater.CamusRequest;

//一个mapper要读取的数据块，包含等待读取的多个topic+partition信息
public class EtlSplit extends InputSplit implements Writable {
  private List<CamusRequest> requests = new ArrayList<CamusRequest>();//等待读取的多个topic+partition信息
  private long length = 0;//所有topic+partition的信息占用的总字节(预估值)
  private String currentTopic = "";//当前在读取的topic信息

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      CamusRequest r = new EtlRequest();
      r.readFields(in);
      requests.add(r);
      length += r.estimateDataSize();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(requests.size());
    for (CamusRequest r : requests)
      r.write(out);
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  //要读取多少个topic+partition信息
  public int getNumRequests() {
    return requests.size();
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  public void addRequest(CamusRequest request) {
    requests.add(request);
    length += request.estimateDataSize();
  }

  //弹出下一个要读取的topic+partition
  public CamusRequest popRequest() {
    if (requests.size() > 0) {
      for (int i = 0; i < requests.size(); i++) {
        // return all request for each topic before returning another topic
        if (requests.get(i).getTopic().equals(currentTopic))
          return requests.remove(i);
      }
      CamusRequest cr = requests.remove(requests.size() - 1);
      currentTopic = cr.getTopic();
      return cr;
    } else
      return null;
  }
}
