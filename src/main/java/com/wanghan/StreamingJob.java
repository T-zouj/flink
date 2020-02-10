/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wanghan;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

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
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Quote>() {
			@Override
			public long extractAscendingTimestamp(Quote quote) {
				return quote.getTimestamp();
			}
		});

		QuoteWMA.caculateWMA(dataStreamSource, Time.seconds(1), "WMA1s");
		QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(1), "WMA1min");
		QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(5), "WMA5min");
		QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(10), "WMA10min");
		QuoteWMA.caculateWMA(dataStreamSource, Time.minutes(30), "WMA30min");
		QuoteWMA.caculateWMA(dataStreamSource, Time.hours(1), "WMA1hour");


		QuoteMA.caculateMA(dataStreamSource, Time.seconds(1), "MA1s");
		QuoteMA.caculateMA(dataStreamSource, Time.minutes(1), "MA1min");
		QuoteMA.caculateMA(dataStreamSource, Time.minutes(5), "MA5min");
		QuoteMA.caculateMA(dataStreamSource, Time.minutes(10), "MA10min");
		QuoteMA.caculateMA(dataStreamSource, Time.minutes(30), "MA30min");
		QuoteMA.caculateMA(dataStreamSource, Time.hours(1), "MA1hour");


		env.execute("MA and WMA");
	}
}
