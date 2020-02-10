package com.wanghan;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class QuoteWMA {

    static Logger logger = LoggerFactory.getLogger(QuoteWMA.class);

    static class WMA {
        private double bidAvgValue;
        private double askAvgValue;
        private String bondCode;
        private long timestamp;
        private String WMAType;

        @Override
        public String toString(){
            return JSON.toJSONString(this);
        }


        public String getBondCode() {
            return bondCode;
        }

        public void setBondCode(String bondCode) {
            this.bondCode = bondCode;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getBidAvgValue() {
            return bidAvgValue;
        }

        public void setBidAvgValue(double bidAvgValue) {
            this.bidAvgValue = bidAvgValue;
        }

        public double getAskAvgValue() {
            return askAvgValue;
        }

        public void setAskAvgValue(double askAvgValue) {
            this.askAvgValue = askAvgValue;
        }

        public String getWMAType() {
            return WMAType;
        }

        public void setWMAType(String WMAType) {
            this.WMAType = WMAType;
        }
    }



    static public DataStream<Quote> getDataSource(StreamExecutionEnvironment env, String topic){

        DataStream<Quote> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                topic,  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                KafkaConfig.getConfig())).map(new MapFunction<String, Quote>() {
            @Override
            public Quote map(String s) throws Exception {
                return JSON.parseObject(s, Quote.class);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Quote>() {
            @Override
            public long extractAscendingTimestamp(Quote quote) {
                return quote.getTimestamp();
            }
        });
        return dataStreamSource;
    }

    static public StreamExecutionEnvironment getStreamExecutionEnvironment(){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    static private DataStream<Quote> getDataStream(DataStream<Quote> dataStreamSource, Time size){
        DataStream<Quote> dataStream = dataStreamSource.keyBy(
                new KeySelector<Quote, String>() {
                    @Override
                    public String getKey(Quote quote) throws Exception {
                        return quote.getBondCode();
                    }
                }).timeWindow(size).reduce(new ReduceFunction<Quote>() {
            @Override
            public Quote reduce(Quote quote, Quote t1) throws Exception {
                logger.info(quote.toString());
                return t1;
            }
        }).name("reduce operator");
        return dataStream;
    }


    public static class WMAcaculate extends ProcessWindowFunction<Quote, WMA, String, GlobalWindow> {

        String WMAType;

        public WMAcaculate(String WMAType){
            this.WMAType = WMAType;
        }

        @Override
        public void process(String s, Context context, Iterable<Quote> iterable, Collector<WMA> collector) throws Exception {
            WMA wma = new WMA();
            wma.setBondCode(s);
            wma.setWMAType(WMAType);
            wma.setTimestamp(context.currentProcessingTime());
            double askAvgValue = 0;
            double bidAvgValue = 0;
            long count = 0;
            long total = 0;
            for(Quote quote: iterable){
                count++;
                askAvgValue+=quote.getAskPrice()*count;
                bidAvgValue+=quote.getBidPrice()*count;
                total+=count;
            }
            wma.setAskAvgValue(askAvgValue/total);
            wma.setBidAvgValue(bidAvgValue/total);
            collector.collect(wma);
        }
    }

    public static class QuoteKeySelector implements KeySelector<Quote, String> {

        @Override
        public String getKey(Quote quote) throws Exception {
            return quote.getBondCode();
        }
    }


    public static void caculateWMA(DataStream<Quote> dataStreamSource,  Time size, String WMAType){
        DataStream<Quote> dataStream = getDataStream(dataStreamSource, size);
        dataStream.keyBy(new QuoteKeySelector()).countWindow(QuoteWMA.count, 1).process(new WMAcaculate(WMAType))
                .addSink(new FlinkKafkaProducer<>("WMA", new KafkaSerializationSchema<WMA>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WMA wma, @Nullable Long aLong) {
                        return new ProducerRecord<>("WMA", JSON.toJSONString(wma).getBytes());
                    }
                }, KafkaConfig.getProducerConfig(), FlinkKafkaProducer.Semantic.NONE)).name("WMA operator");
    }

    static public int count = 5;


//    public static void main(String[] args) throws Exception {
//        // set up the streaming execution environment
//
//        logger.warn("test");
//        StreamExecutionEnvironment env = QuoteWMA.getStreamExecutionEnvironment();
//        DataStream<Quote> dataStreamSource = QuoteWMA.getDataSource(env, "quote");
//
//        QuoteWMA.caculateWMA(dataStreamSource, Time.seconds(1), "WMA1s");
//        QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(1), "WMA1min");
//        QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(5), "WMA5min");
//        QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(10), "WMA10min");
//        QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(30), "WMA30min");
//        QuoteWMA.caculateWMA(dataStreamSource, Time.hours(1), "WMA1hour");
//
//        env.execute("quote WMA");
//
//    }

}
