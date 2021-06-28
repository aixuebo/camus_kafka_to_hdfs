package com.linkedin.camus.etl.kafka.common;

import java.net.URI;


/**
 * Model class to store the leaderInformation
 * @author ggupta
 * 表示 topic-partition的一个leader信息
 *
 */

public class LeaderInfo {

  private URI uri;//new URI("tcp://leader节点")
  private int leaderId;//leaderId

  public LeaderInfo(URI uri, int leaderId) {
    this.uri = uri;
    this.leaderId = leaderId;
  }

  public int getLeaderId() {
    return leaderId;
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public int hashCode() {
    return this.uri.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return this.hashCode() == obj.hashCode();
  }
}
