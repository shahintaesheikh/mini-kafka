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
    public static ByteBuffer encodeTopicNotification(String topic, int partition, byte[] message)
}