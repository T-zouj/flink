package com.wanghan;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
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

public class QuoteMA {

    static Logger logger = LoggerFactory.getLogger(QuoteMA.class);


    static class MA {
        private double bidAvgValue;
        private double askAvgValue;
        private String bondCode;
        private long timestamp;
        private String MAType;

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

        public String getMAType() {
            return MAType;
        }

        public void setMAType(String MAType) {
            this.MAType = MAType;
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
                return t1;
            }
        }).name("reduce operator");
        return dataStream;
    }


    public static class MAcaculate extends ProcessWindowFunction<Quote, MA, String, GlobalWindow> {

        String MAType;

        public MAcaculate(String MAType){
            this.MAType = MAType;
        }

        @Override
        public void process(String s, Context context, Iterable<Quote> iterable, Collector<MA> collector) throws Exception {
            MA ma = new MA();
            ma.setBondCode(s);
            ma.setMAType(MAType);
            ma.setTimestamp(context.currentProcessingTime());
            double askAvgValue = 0;
            double bidAvgValue = 0;
            long count = 0;
            for(Quote quote: iterable){
                askAvgValue+=quote.getAskPrice();
                bidAvgValue+=quote.getBidPrice();
                count++;
            }
            ma.setAskAvgValue(askAvgValue/count);
            ma.setBidAvgValue(bidAvgValue/count);
            collector.collect(ma);
        }
    }

    public static class QuoteKeySelector implements KeySelector<Quote, String> {

        @Override
        public String getKey(Quote quote) throws Exception {
            return quote.getBondCode();
        }
    }


    public static void caculateMA(DataStream<Quote> dataStreamSource,  Time size, String MAType){
        DataStream<Quote> dataStream = getDataStream(dataStreamSource, size);
        dataStream.keyBy(new QuoteKeySelector()).countWindow(QuoteMA.count, 1).process(new MAcaculate(MAType))
                .addSink(new FlinkKafkaProducer<>("MA", new KafkaSerializationSchema<MA>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(MA ma, @Nullable Long aLong) {
                        return new ProducerRecord<>("MA", JSON.toJSONString(ma).getBytes());
                    }
                }, KafkaConfig.getProducerConfig(), FlinkKafkaProducer.Semantic.NONE));
    }

    static public int count = 5;


//    public static void main(String[] args) throws Exception {
//        // set up the streaming execution environment
//        logger.warn("test");
//
//        StreamExecutionEnvironment env = QuoteMA.getStreamExecutionEnvironment();
//        DataStream<Quote> dataStreamSource = QuoteMA.getDataSource(env, "quote");
//
//
//
//
//        QuoteMA.caculateMA(dataStreamSource, Time.seconds(1), "MA1s");
//        QuoteMA.caculateMA(dataStreamSource, Time.minutes(1), "MA1min");
//        QuoteMA.caculateMA(dataStreamSource, Time.minutes(5), "MA5min");
//        QuoteMA.caculateMA(dataStreamSource, Time.minutes(10), "MA10min");
//        QuoteMA.caculateMA(dataStreamSource, Time.minutes(30), "MA30min");
//        QuoteMA.caculateMA(dataStreamSource, Time.hours(1), "MA1hour");
//
//        env.execute("quote MA");
//
//    }

}
