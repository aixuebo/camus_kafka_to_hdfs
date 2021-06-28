package com.linkedin.camus.etl;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

//用于存储hadoop中处理的key,用于在节点间互相传递
public interface IEtlKey {
    String getServer();

    String getService();

    long getTime();

    String getTopic();

    //String getNodeId();

    int getPartition();

    long getBeginOffset();

    long getOffset();

    long getChecksum();
    
    long getMessageSize();

    MapWritable getPartitionMap();

    void put(Writable key, Writable value);
}
