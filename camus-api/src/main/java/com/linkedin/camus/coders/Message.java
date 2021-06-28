package com.linkedin.camus.coders;

import java.io.IOException;

/**
 * Created by michaelandrepearce on 05/04/15.
 * 保存接受到的一个信息内容:topic、partition、offset、key、value、checksum
 */
public interface Message {
    byte[] getPayload();//value

    byte[] getKey();

    String getTopic();

    long getOffset();

    int getPartition();

    long getChecksum();

    void validate() throws IOException;
}
