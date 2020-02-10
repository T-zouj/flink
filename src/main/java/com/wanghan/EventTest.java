package com.wanghan;

import com.alibaba.fastjson.JSON;
import com.sun.org.apache.xpath.internal.operations.Quo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class EventTest {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Quote> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "quote",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                KafkaConfig.getConfig())).map(new MapFunction<String, Quote>() {
            @Override
            public Quote map(String s) throws Exception {
                return JSON.parseObject(s, Quote.class);
            }
        }).filter(new FilterFunction<Quote>() {
            @Override
            public boolean filter(Quote quote) throws Exception {
                return true;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Quote>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Quote quote) {
                return quote.getTimestamp();
            }
        });




//                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Quote>() {
//            private long currentMaxTimestamp;
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                System.out.println("getCurrentWatermark:"+currentMaxTimestamp);
//                return new Watermark(currentMaxTimestamp);
//            }
//
//            @Override
//            public long extractTimestamp(Quote quote, long l) {
//                long timestamp = quote.getTimestamp();
//                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
////                System.out.println("currentMaxTimestamp:"+currentMaxTimestamp);
////                System.out.println("timestamp:"+timestamp);
////                System.out.println("systemTimestamp:"+System.currentTimeMillis());
//                return timestamp;
//            }
//        });


        DataStream<Quote> newStream = dataStreamSource.timeWindowAll(Time.seconds(2)).process(new ProcessAllWindowFunction<Quote, Quote, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Quote> iterable, Collector<Quote> collector) throws Exception {
                //System.out.println("process start");
                for(Quote ele: iterable){
                    System.out.println(JSON.toJSONString(ele));
                    collector.collect(ele);
                    return;
                }
                //System.out.println("process end");
            }
        });

        newStream.timeWindowAll(Time.seconds(2)).process(new ProcessAllWindowFunction<Quote, Object, TimeWindow>() {

            @Override
            public void process(Context context, Iterable<Quote> iterable, Collector<Object> collector) throws Exception {
                for(Quote ele: iterable){
                    System.out.println(JSON.toJSONString(ele));
                    //collector.collect(ele);
                    //return;
                }
            }
        });

        //dataStreamSource.print(); //把从 kafka
        env.execute("event test");

    }

}
