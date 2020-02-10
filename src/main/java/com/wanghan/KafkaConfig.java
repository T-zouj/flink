package com.wanghan;

import java.util.Properties;

public class KafkaConfig {


    static public Properties getConfig(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//		props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化
        return props;
    }

    static public Properties getProducerConfig(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transaction.timeout.ms", 5000);
//		props.put("zookeeper.connect", "localhost:2181");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
