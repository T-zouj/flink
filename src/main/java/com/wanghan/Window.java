package com.wanghan;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.xpath.internal.operations.Quo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class Window {


    public static class test implements SessionWindowTimeGapExtractor<Quote>{

        @Override
        public long extract(Quote o) {
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
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


//        dataStreamSource.keyBy(new KeySelector<Quote, Object>() {
//            @Override
//            public Object getKey(Quote quote) throws Exception {
//                return quote.getSrno();
//            }
//        }).window(TumblingEventTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<Quote, Object, Object, TimeWindow>() {
//
//            @Override
//            public void process(Object o, Context context, Iterable<Quote> iterable, Collector<Object> collector) throws Exception {
//                System.out.println("process");
//            }
//        });

//        dataStreamSource.keyBy(new KeySelector<Quote, Object>() {
//            @Override
//            public Object getKey(Quote quote) throws Exception {
//                return quote.getSrno();
//            }
//        }).window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Quote>() {
//            @Override
//            public long extract(Quote o) {
//                return 0;
//            }
//        })).process(new ProcessWindowFunction<Quote, Object, Object, TimeWindow>() {
//
//            @Override
//            public void process(Object o, Context context, Iterable<Quote> iterable, Collector<Object> collector) throws Exception {
//
//            }
//        });


        dataStreamSource.keyBy(new KeySelector<Quote, Object>() {
            @Override
            public Object getKey(Quote quote) throws Exception {
                return quote.getSrno();
            }
        }).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Quote, Object, Object, TimeWindow>() {

            @Override
            public void process(Object o, Context context, Iterable<Quote> iterable, Collector<Object> collector) throws Exception {
                System.out.println(context.globalState());
                System.out.println(context.windowState());
                System.out.println(context.window().getEnd());
            }
        });

//        dataStreamSource.keyBy(new KeySelector<Quote, Object>() {
//
//            @Override
//            public Object getKey(Quote quote) throws Exception {
//                return quote.getSrno();
//            }
//        }).process();


        //dataStreamSource.print(); //把从 kafka
        env.execute("event test");

    }
}
