通过property文件启动任务。入口CamusJob

1.camus.message.decoder.class.$topicName = 设置如何对topic的数据信息处理。比如是做json还是其他方式。
camus.message.decoder.class = 默认格式
即可以为每一个topic设置不同的输出格式


一、EtlInputFormat
将文件拆分成InputSplit。 以及 读取某一个Split

1.获取所有topic的元数据信息 以及 每一个topic有多少个partition，一个每一个partition的leader节点是谁
String brokerString = CamusJob.getKafkaBrokers(context);
打乱,向某一个broker节点发送topic元数据请求，获取元数据集合 List<TopicMetadata> topicMetadataList


class TopicMetadata(private val underlying : kafka.api.TopicMetadata) extends scala.AnyRef {
  def topic : scala.Predef.String = { /* compiled code */ }
  def partitionsMetadata : java.util.List[kafka.javaapi.PartitionMetadata] = { /* compiled code */ }



  class PartitionMetadata(private val underlying : kafka.api.PartitionMetadata) extends scala.AnyRef {
    def partitionId : scala.Int = { /* compiled code */ }
    def leader : kafka.cluster.Broker = { /* compiled code */ }
    def replicas : java.util.List[kafka.cluster.Broker] = { /* compiled code */ }
    def isr : java.util.List[kafka.cluster.Broker] = { /* compiled code */ }


2.topic过滤
保留的topic规则:一定在白名单规则里 && 一定不能在黑名单规则里。
即黑名单的topic一定不能被同步。
白名单的规则topic，是在同步的池子里作潜在同步集合。


3.自动识别topic的所有partition。

循环每一个topic+partition,记录每一个leader节点上有哪些topic+partition
LeaderInfo leader = new LeaderInfo(new URI("tcp://partition的leader节点"),partitionMetadata.leader().id());

//因为每一个topic+partition分布在不同的leader节点上,因此该信息是汇总每一个leader节点有哪些topic+partition数据
//key 是leader节点，value是该节点上的topic+partition集合
HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo = new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();


4.去请求每一个leader节点，获取该节点上topic+partition的最新、最老offset信息，返回ArrayList<CamusRequest>
a.连接leader节点。
b.发送获获取topic+partition的offset请求。
3.返回List<CamusRequest>,每一个topic+partition对应一个CamusRequest对象
CamusRequest etlRequest = new EtlRequest(context, topic, LeaderId(),partitionId,leader的URL)
etlRequest.setLatestOffset(latestOffset);
etlRequest.setEarliestOffset(earliestOffset); 更新能查询的offset范围。

同步到文件中。文件名称requests.previous，文件内容是CamusRequest序列化。

5.设置要读取的offset开始位置。
a.从数据源上，读取每一个topic+partition已经同步到哪个offset了。
数据内容是EtlKey，通过EtlKey返回Map<CamusRequest, EtlKey>。
Map<CamusRequest, EtlKey> offsetKeysMap = new HashMap<CamusRequest, EtlKey>();
CamusRequest request = new EtlRequest(context, key.getTopic(), key.getLeaderId(), key.getPartition());
offsetKeysMap.put(request, key);
b.有一些需求,要求某些topic强制从最新数据开始读取，因此需要配置key "kafka.move.to.last.offset.list"; 满足该条件的topic都会从最新的offset开始读取数据。
当value为all时候，表示全部topic都从最新的offset数据开始读取。

将最终的数据EtlKey，同步到文件中，文件名-previous

6.将List<CamusRequest>分配到多个hadoop节点上去同步数据。
目标让不同的hadoop节点抓取相对平均的数据量。
即每一个hadoop节点请求kafka的leader节点，抓取多个topic+partition数据。

WorkAllocator.allocateWork(finalRequests, context);//分配若干个数据块


二、BaseAllocator

//如何对待抓取的kafka数据进行分组，分到不同节点去抓取。
//因为kafka以topic+partition方式去抓取数据，因此为了并行度更高，可以让一个节点抓取多个topic+partition

根据task并行度，设置若干个InputSplit对象，算法保证进来每一个InputSplit要抓取的数据量是平衡的，即谁数据量越小，每次添加到数据量最小的InputSplit中即可


//一个mapper要读取的数据块，包含等待读取的多个topic+partition信息
public class EtlSplit extends InputSplit implements Writable {
  private List<CamusRequest> requests = new ArrayList<CamusRequest>();//等待读取的多个topic+partition信息

三、class EtlRecordReader extends RecordReader<EtlKey, CamusWrapper> 如何读取某一个分区
1.每一个hadoop节点,依次循环处理若干个topic+partition数据。
每一次请求对应的leader节点,获取具体的信息。信息会进入mapper方法进一步处理。

2.控制参数
a.每一个topic+partition最多允许处理多久,比如设置1分钟，如果读取一个topic+partition超过了1分钟，则不再读取该partition，切换到下一个partition处理
b.如果设置了参数 kafka.max.pull.hrs,则endTimeStamp会最终为第一次处理topic+partition时拿到的事件时间戳+kafka.max.pull.hrs对应的延迟时间。即更新endTimeStamp

3.解析数据的时候。可以获取到信息：
EtlKey = （request.getTopic(), request.getLeaderId(), request.getPartition(), request.getOffset(),从要读取的offset, checksum）
Message = 是真正读取到的kafka信息。下一个大逻辑讲这部分。
message.validate(); 做校验和校验。

4.对message信息转换成hdfs上存储的格式。CamusWrapper，基于decode方式编码。

5.设置EtlKey的额外信息，用于mapper方法处理时使用。
key.setTime(curTimeStamp);//kafka信息中的事件时间戳
key.addAllPartitionMap(wrapper.getPartitionMap());//kafka信息中额外的信息


四、KafkaReader真正如何读取一个kafka的partition内容
1.从请求中获取partition的最新、最老offset，确定可以读取的范围，获取从哪个位置开始读取数据
beginOffset = EtlRequest.getOffset(); 从哪个位置开始读取
currentOffset = EtlRequest.getOffset(); 目前读取到哪个位置了
lastOffset = EtlRequest.getLastOffset();

2.连接leader节点，抓取topic+partition的数据，通知从哪个offset位置开始抓取数据
每次批量抓取N条数据，不断的循环抓取
返回 Iterator<MessageAndOffset> messageIter = null;//记录每一个offset和对应的message信息,message中包含了key和value

3.
public KafkaMessage getNext(EtlKey etlKey) throws IOException {
    if (hasNext()) {

      MessageAndOffset msgAndOffset = messageIter.next();
      Message message = msgAndOffset.message();

      byte[] payload = getBytes(message.payload());
      byte[] key = getBytes(message.key());


      etlKey.clear();
      etlKey.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(), currentOffset(当前位置),
          msgAndOffset.offset() + 1, message.checksum());

      etlKey.setMessageSize(msgAndOffset.message().size());

      currentOffset = msgAndOffset.offset() + 1; // increase offset

//包含主要的信息  topic、partition、key、value、当前offset
      return new KafkaMessage(payload, key, kafkaRequest.getTopic(), kafkaRequest.getPartition(),msgAndOffset.offset(), message.checksum());


    } else {
      return null;
    }
  }

五、partition
根据不同的时间范围，创建唯一的名称。

六、EtlMultiOutputRecordWriter 输出如何输出HDFS上
1.不同的key，产生不同的目录文件：根据topic、leader的brokerId、partitionId、partition编码后的文件名字。
可以确保本地文件名唯一性。

2.向温度文件写入数据。

3.提交offset缓存。

4.在最终commit的时候，会保存offset


