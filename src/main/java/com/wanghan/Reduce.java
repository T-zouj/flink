package com.wanghan;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class Reduce {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Quote> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "quote",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                KafkaConfig.getConfig())).map(new MapFunction<String, Quote>() {
            @Override
            public Quote map(String s) throws Exception {
                return JSON.parseObject(s, Quote.class);
            }
        });

        dataStreamSource.keyBy(new KeySelector<Quote, String>() {
            @Override
            public String getKey(Quote quote) throws Exception {
                return quote.getSrno();
            }
        }).timeWindow(Time.seconds(1)).reduce(new ReduceFunction<Quote>() {
            @Override
            public Quote reduce(Quote quote, Quote t1) throws Exception {
                System.out.println(quote.hashCode());
                System.out.println(t1.hashCode());
                return quote;
            }
        }).print();

        dataStreamSource.keyBy(new KeySelector<Quote, String>() {
            @Override
            public String getKey(Quote quote) throws Exception {
                return quote.getSrno();
            }
        }).flatMap(new RichFlatMapFunction<Quote, Object>() {

            @Override
            public void flatMap(Quote quote, Collector<Object> collector) throws Exception {

            }
        });


        env.execute("operator test");

    }
}
