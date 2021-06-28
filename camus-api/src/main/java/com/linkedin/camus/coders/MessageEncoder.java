package com.linkedin.camus.coders;

import java.util.Properties;
//如何将R转换成M
public abstract class MessageEncoder<R, M> {
    protected Properties props;
    protected String topicName;

    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;
    }

    public abstract byte[] toBytes(R record);
    
}
