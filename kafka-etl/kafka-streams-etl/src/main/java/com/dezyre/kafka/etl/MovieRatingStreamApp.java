/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafka.etl;

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
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;

import com.dezyre.kafka.etl.model.MovieRating;
import com.dezyre.kafka.etl.model.Rating;
import com.fasterxml.jackson.databind.JsonNode;

/**
 *
 * @author m.enudi
 */
public class MovieRatingStreamApp {

	public static void main(String[] args) {
		Properties prop = new Properties();

		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "MovieRatingStreamApp");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092");
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()
				.getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes
				.String().getClass());

		KStreamBuilder kStreamBuilder = new KStreamBuilder();

		// get the movies streams
		KStream<Long, String> movieRatingStream = kStreamBuilder
				.stream(Serdes.String(), Serdes.String(), "ml-movies-in")
				.filter(new Predicate<String, String>() {
					@Override
					public boolean test(String k, String v) {
						return !v.startsWith("movie"); // the header
					}
				})
				.map(new KeyValueMapper<String, String, KeyValue<Long, MovieRating>>() {
					@Override
					public KeyValue<Long, MovieRating> apply(String k, String v) {
						MovieRating movieRating = MovieRating
								.fromMovieString(v);
						return new KeyValue<>(movieRating.getMovieId(),
								movieRating);
					}
				}).mapValues(new ValueMapper<MovieRating, String>() {
					@Override
					public String apply(MovieRating v) {
						return v.toString();
					}
				});
		// write movies to the stream
		movieRatingStream.to(Serdes.Long(), Serdes.String(),
				"ml-movies-rating-in");

		// read the movie rating back as a globalKTable
		GlobalKTable<Long, String> movieRatingKTable = kStreamBuilder
				.globalTable(Serdes.Long(), Serdes.String(),
						"ml-movies-rating-in");

		// stream the ratings
		KStream<Long, Rating> ratingStream = kStreamBuilder
				.stream(Serdes.String(), Serdes.String(), "ml-ratings-in")
				.filter(new Predicate<String, String>() {
					@Override
					public boolean test(String k, String v) {
						return !v.contains("userId"); // the header
					}
				})
				.map(new KeyValueMapper<String, String, KeyValue<Long, Rating>>() {
					@Override
					public KeyValue<Long, Rating> apply(String k, String v) {
						Rating rating = Rating.fromString(v);
						return new KeyValue<>(rating.getMovieId(), rating);
					}
				});

		KStream<Long, JsonNode> joinedStream = ratingStream.join(
				movieRatingKTable, new KeyValueMapper<Long, Rating, Long>() {
					@Override
					public Long apply(Long k, Rating v) {
						return k;
					}
				}, new ValueJoiner<Rating, String, JsonNode>() {
					@Override
					public JsonNode apply(Rating rating, String v2) {
						MovieRating movieRating = MovieRating.fromString(v2)
								.addRating(rating);
						return movieRating.toJsonNode();
					}
				});

		Serializer<JsonNode> jsonSerializer = new JsonSerializer();
		Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
		Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer,
				jsonDeserializer);

		// write out this new value to output
		joinedStream.to(Serdes.Long(), jsonSerde, "ml-denor-movie-rating");

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
}
