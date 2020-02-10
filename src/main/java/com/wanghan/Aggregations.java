package com.wanghan;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class Aggregations {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Quote> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "quote",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                KafkaConfig.getConfig())).map(new MapFunction<String, Quote>() {
            @Override
            public Quote map(String s) throws Exception {
                Quote quote = JSON.parseObject(s, Quote.class);
                //System.out.println("map:"+quote.hashCode());
                return quote;
            }
        });

//        dataStreamSource.keyBy(new KeySelector<Quote, String>() {
//            @Override
//            public String getKey(Quote quote) throws Exception {
//                //System.out.println("getKey:"+quote.hashCode());
//                return "111";
//            }
//        }).sum("price").print();


        dataStreamSource.connect(dataStreamSource);

        env.execute("aggregation test");

    }
}
