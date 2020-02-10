//package com.wanghan;
//
//import com.alibaba.fastjson.JSON;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//public class Fold {
//
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Quote> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
//                "quote",  //kafka topic
//                new SimpleStringSchema(),  // String 序列化
//                KafkaConfig.getConfig())).map(new MapFunction<String, Quote>() {
//            @Override
//            public Quote map(String s) throws Exception {
//                return JSON.parseObject(s, Quote.class);
//            }
//        });
//
//        dataStreamSource.keyBy(new KeySelector<Quote, String>() {
//            @Override
//            public String getKey(Quote quote) throws Exception {
//                return quote.getSrno();
//            }
//        }).fold("start")
//
//
//        env.execute("fold test");
//
//    }
//
//}
