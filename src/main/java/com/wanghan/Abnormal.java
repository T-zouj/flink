package com.wanghan;

//异常监测



//1.一段时间内 大量报价 但是又大量撤销，成交很少量或者无成交

//2.虚假反向成交：机构在自己希望成交的交易方向的相反方向放大额的订单，营造虚假的市场供求情况，误导其他机构在更优的价格上放置订单，
// 并在基本不成交或者少量成交的情况下，撤销已放置的大额订单并以相反方向成交
// （通过点击或者更早的时候在相反方向放置真实意愿的订单），
// 以在希望成交的交易方向获得更优的成交价格。

//3.相同机构提交优于市场最优价的一笔订单，当提交的订单价格与市场最优价的点差达到虚假少量订单价格点差，
// 且该笔订单的订单量小于虚假少量订单量，则形成预警，该笔订单为虚假少量订单

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class Abnormal {


//    public void Abnormal(StreamExecutionEnvironment env){
//        this.env = env;
//    }


    //1.一段时间内 大量报价 但是又大量撤销，成交很少量或者无成交
    public void Monitor1(StreamExecutionEnvironment env){

        DataStream<Tuple2<QuoteDetail, Deal>> quoteDetailDataStream = env.addSource(new FlinkKafkaConsumer<>(
                "QuoteDetail",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                KafkaConfig.getConfig())).map(new MapFunction<String, Tuple2<QuoteDetail,Deal>>() {
            @Override
            public Tuple2<QuoteDetail,Deal> map(String s) throws Exception {
                return new Tuple2<>(JSON.parseObject(s, QuoteDetail.class), null);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<QuoteDetail,Deal>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<QuoteDetail,Deal> quoteDetail) {
                return quoteDetail.f0.getTimestamp();
            }
        });

        DataStream<Tuple2<QuoteDetail, Deal>> dealDataStream = env.addSource(new FlinkKafkaConsumer<>(
                "Deal",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                KafkaConfig.getConfig())).map(new MapFunction<String, Tuple2<QuoteDetail, Deal>>() {
            @Override
            public Tuple2<QuoteDetail, Deal> map(String s) throws Exception {
                return new Tuple2<QuoteDetail, Deal>(null, JSON.parseObject(s, Deal.class));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<QuoteDetail, Deal>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<QuoteDetail, Deal> dealDetail) {
                return dealDetail.f1.getTimestamp();
            }
        });

        DataStream<Tuple2<QuoteDetail, Deal>> quoteAndDealStream = quoteDetailDataStream.union(dealDataStream);

        quoteAndDealStream.keyBy(new KeySelector<Tuple2<QuoteDetail,Deal>, Object>() {

            @Override
            public String getKey(Tuple2<QuoteDetail, Deal> quoteDetailDealTuple2) throws Exception {
                if(quoteDetailDealTuple2.f0 != null){
                    return quoteDetailDealTuple2.f0.getBondCode()
                            +quoteDetailDealTuple2.f0.getEntityCode()
                            +quoteDetailDealTuple2.f0.getDirection();
                }else if(quoteDetailDealTuple2.f1 != null){
                    return quoteDetailDealTuple2.f1.getBondCode()
                            +quoteDetailDealTuple2.f1.getEntityCode()
                            +quoteDetailDealTuple2.f1.getDirection();
                }else {
                    return null;
                }
            }

        }).timeWindow(Time.seconds(60)).process(new ProcessWindowFunction<Tuple2<QuoteDetail,Deal>, Object, Object, TimeWindow>() {

            @Override
            public void process(Object o, Context context, Iterable<Tuple2<QuoteDetail, Deal>> iterable, Collector<Object> collector) throws Exception {
                //计算某机构这段时间内对某产品多少报价
                //计算某机构这段时间内对某产品多少撤单
                //计算某机构这段时间内对某产品产生多少成交
                //比值是多少
            }
        });


    }

}
