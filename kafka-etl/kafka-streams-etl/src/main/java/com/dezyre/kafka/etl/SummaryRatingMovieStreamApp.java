/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafka.etl;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.dezyre.kafka.etl.model.MovieRating;
import com.dezyre.kafka.etl.model.SummaryRating;
import com.fasterxml.jackson.databind.JsonNode;

/**
 *
 * @author m.enudi
 */
public class SummaryRatingMovieStreamApp {

	public static void main(String[] args) {

		Properties prop = new Properties();

		MyJsonSerdes jsonSerde = new MyJsonSerdes();

		prop.put("application.id", "SummaryRatingMovieStreamApp");
		prop.put("bootstrap.servers", "quickstart.cloudera:9092");
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long()
				.getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				jsonSerde.getClass());

		KStreamBuilder kStreamBuilder = new KStreamBuilder();

		KTable<Long, MovieRating> movieRatingTable = kStreamBuilder
				.table(Serdes.Long(), jsonSerde, "ml-denor-movie-rating")
				.mapValues(new ValueMapper<JsonNode, MovieRating>() {
					@Override
					public MovieRating apply(JsonNode jsonNode) {
						return MovieRating.fromJsonNode(jsonNode);
					}
				}).filter(new Predicate<Long, MovieRating>() {
					@Override
					public boolean test(Long key, MovieRating arg1) {
						return arg1.getMovieId() != null;
					}
				});

		KTable<Long, JsonNode> resultTable = movieRatingTable
				.groupBy(
						new KeyValueMapper<Long, MovieRating, KeyValue<Long, JsonNode>>() {
							@Override
							public KeyValue<Long, JsonNode> apply(Long k,
									MovieRating v) {
								SummaryRating sr = new SummaryRating();
								sr.setMovieId(v.getMovieId());
								sr.setRatingCount(1);
								sr.setRatingTotal(v.getRating());
								sr.setTitle(v.getTitle());
								sr.setYear(v.getYear());
								return new KeyValue<Long, JsonNode>(k, sr
										.toJsonNode());
							}
						}).reduce(new SummaryRatingMerger(),
						new SummaryRatingSubtrater());

		resultTable.to(Serdes.Long(), jsonSerde, "ml-rating-summary");

		final KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, prop);
		kafkaStreams
				.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
					@Override
					public void uncaughtException(Thread t, Throwable e) {
						e.printStackTrace(System.out);
					}
				});

		System.out.println(kafkaStreams.toString("->"));
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				kafkaStreams.close();
			}
		}));

	}

	private static class SummaryRatingMerger implements Reducer<JsonNode> {

		@Override
		public JsonNode apply(JsonNode jsonNode1, JsonNode jsonNode2) {

			System.out.println("SummaryRatingMerger :::: " + jsonNode1);
			System.out.println("SummaryRatingMerger :::: " + jsonNode2);

			SummaryRating v = SummaryRating.fromJsonNode(jsonNode1);
			SummaryRating v1 = SummaryRating.fromJsonNode(jsonNode2);
			return v.mergeSummaries(v1).toJsonNode();
		}
	}

	private static class SummaryRatingSubtrater implements Reducer<JsonNode> {

		@Override
		public JsonNode apply(JsonNode jsonNode1, JsonNode jsonNode2) {
			return jsonNode1;
		}

	}

	public static class MyJsonSerdes implements Serde<JsonNode> {

		private JsonSerializer serializer;
		private JsonDeserializer deserializer;

		public MyJsonSerdes() {
			this.serializer = new JsonSerializer();
			this.deserializer = new JsonDeserializer();
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			serializer.configure(configs, isKey);
			deserializer.configure(configs, isKey);
		}

		@Override
		public void close() {
			serializer.close();
			deserializer.close();
		}

		@Override
		public Serializer<JsonNode> serializer() {
			return serializer;
		}

		@Override
		public Deserializer<JsonNode> deserializer() {
			return deserializer;
		}

	}

}
