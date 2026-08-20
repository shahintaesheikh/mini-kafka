package com.minikafka.broker;

//wire protocol for how nodes brokers communicate
public class Protocol{
    //client request types
    public static int final byte PRODUCE = 0x01;
    public static int final byte FETCH = 0x02;
    public static int final byte METADATA = 0x03;
    public static int final byte CREATE_TOPIC = 0x04;

    //broker response types
    public static int final byte PRODUCE_RESPONSE = 0x11;
    public static int final byte FETCH_RESPONSE = 0x12;
    public static int final byte METADATA_RESPONSE = 0x13;
    public static int final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static int final byte ERROR_RESPONSE = 0x15;

    //internal broker communication
    public static int final byte REPLICATE = 0x21;
    public static int final byte REPLICATE_ACK = 0x22;
    public static int final byte TOPIC_NOTIFICATION = 0x23;

    /*
    Encoding a producer (write) request made by the broker
    */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message){
        ByteBuffer buffer = ByteBuffer.allocate(11+topic.length()+message.length())
        
        // 1 byte
        buffer.put(PRODUCE);
        //short -> 2 byte num
        buffer.putShort((short) topic.length());
        //
        buffer.put(topic.getBytes());
        //4 bytes for int
        buffer.putInt(partition);
        //another 4 bytes for int
        buffer.putInt(message.length);
        //
        buffer.put(message);
        buffer.flip()
        return buffer;
    }

    /*
    Encodes fetch (read) request from cluster
    */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes){
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        
        buffer.put(FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        //give me everything starting from position X where X = offset
        buffer.putLong(offset);
        //don't give me more than maxBytes total 
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    /*
    Encodes metadata (what does the cluster look like) fetch request
    */
    public static ByteBuffer encodeMetadataRequest(){
        ByteBuffer buffer = ByteBuffer.allocate();
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /*
    Encodes create topic (group of partitions) request
    */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor){
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);           //1 byte – opcode/tag
        buffer.putShort((short) topic.length());    //2 bytes – the actual number which is the length (not the same as topic.length())
        buffer.put(topic.getBytes());         //accounted for in the allocation – this is actually what topic.length() in the allocation is accounting for
        buffer.putInt(numPartitions);       //4 bytes – integer
        buffer.putShort(replicationFactor);     //2 btytes – short 
        buffer.flip(); 
        return buffer;
    }

    /*
    Encodes a replication (replica of a partition) request
    */
    public static ByteBuffer encodeReplicationRequest(String topic, int partition, long offset, byte[] message){
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length() + message.length);

        buffer.put(REPLICATE);      //1 byte
        buffer.putShort((short) topic.length());        //2 bytes
        buffer.put(topic.getBytes())      //accounted for
        buffer.putInt(partition);       //4 bytes
        buffer.putLong(offset);        //8 bytes
        buffer.putInt(message.length);      //4 bytes
        buffer.put(message);           //accounted for
        buffer.flip();
        return buffer;
    }

    /*
    Encode a topic notification
    */
    public static ByteBuffer encodeTopicNotification(String topic){
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length())
        
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.flip();
        return buffer;
    }

    /*
    Decoding
    */

    /*
    Decoding a fetch response
    */
    public static ByteBuffer decodeFetchResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE){
            //response handling
            if (responseType == ERROR_RESPONSE){
                //check the length of the actual error in terms of byte messages
                short errorLength = buffer.getShort();
                //
                byte[] errorBytes = new byte[errorLength];
                //get the bytes
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                //set to 0 array so that you don't get null pointer exception when someone tries to fetch an error log
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type")
        }

        int messageCount = buffer.getInt();
        //defining array of bytes that has capacity of message count
        byte[][] messages = new byte[messageCount][];

        for (int i = 0; i < messageCount; i++){
            long offset = buffer.getLong();     //skip offset
            int messageSize = buffer.getInt();
            //store message from buffer in messages
            messages[i] = new byte[messageSize];
            //read messageSize amount of bytes from buffer and store in messages[i]
            buffer.get(messages[i])
        }

        return new FetchResult(messages, null)

    }

    public static ByteBuffer = decodeProduceResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
        if(responseType != PRODUCE_RESPONSE){
            if (responseType == ERROR_RESPONSE){
                short errorLength = buffer.getShort();
                byte errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error)
            }
            return new ProduceResult(-1, "Invalid response type")
        }

        long offset = buffer.getLong();         //skip offset
        byte status = buffer.get();

        return new ProductResult(offset, status == 0 ? null: "Produce failed")
    }

    public static ByteBuffer decodeMetadataResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
        if(responseType != METADATA_RESPONSE){
            if(responseType == ERROR_RESPONSE){
                short errorLength = buffer.getShort();
                byte errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }

        //parse broker info
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>(); 

        for(int i = 0; i < brokerCount; i++){
            int brokerId = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();
            
            //add accumulated info to broker info list
            brokers.add(new BrokerInfo(brokerId, host, port));
        }

        //parse topic metadata
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++){
            short topicLength = broker.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topic = new String(topicBytes);

            //define partitions for this topic that we are going to add to topic
            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();

            for (int j = 0; j < partitionCount; i++){
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();
                
                int replicaCount = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();
                for (int k = 0; i < replicaCount; k++{
                    replicaIds.add(buffer.getInt());
                })

                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds))
            }

            topics.add(new TopicMetadata(topic, partitions))
        }
        
        return new MetadataResult(brokers, topics, null)
    }

    /*
    Result class for produce operations
    */

    public static class ProduceResult{
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error){
            this.offset = offset;
            this.error = error;
        }

        public long getOffset(){
            return offset;
        }

        public String getError(){
            return error;
        }

        public boolean isSuccess(){
            return error == null;
        }
    }

    public static class FetchResult{
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error){
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages(){
            return messages;
        }

        public int getMessagesLength(){
            return messages.length;
        }

        public String getError(){
            return error;
        }

        public boolean isSuccess(){
            return error == null; 
        }
    }
    
    public static class MetadataResult{
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error){
            this.brokers = brokers; 
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers(){
            return brokers;
        }

        public List<TopicMetadata> getTopics(){
            return topics;
        }

        public String getError(){
            return error;
        }

        public boolean isSuccess(){
            return error==null;
        }
    }
}