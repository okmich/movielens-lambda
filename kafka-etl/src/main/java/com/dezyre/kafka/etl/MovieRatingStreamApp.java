/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafka.etl;

import com.dezyre.kafka.etl.model.MovieRating;
import com.dezyre.kafka.etl.model.Rating;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 *
 * @author m.enudi
 */
public class MovieRatingStreamApp {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put("application.id", "MovieRatingStreamApp");
        prop.put("bootstrap.servers", "localhost:9092");

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        //get the movies streams
        KStream<Long, String> movieRatingStream = kStreamBuilder.stream(Serdes.String(), Serdes.String(), "ml-movies-in").
                filter(new Predicate<String, String>() {
                    @Override
                    public boolean test(String k, String v) {
                        return !v.startsWith("movie"); //the header
                    }
                }).
                map(new KeyValueMapper<String, String, KeyValue<Long, MovieRating>>() {
                    @Override
                    public KeyValue<Long, MovieRating> apply(String k, String v) {
                        MovieRating movieRating = MovieRating.fromString(v);
                        return new KeyValue<>(movieRating.getMovieId(), movieRating);
                    }
                }).
                mapValues(new ValueMapper<MovieRating, String>() {
                    @Override
                    public String apply(MovieRating v) {
                        return v.toString();
                    }
                });
        //write movies to the stream
        movieRatingStream.to(Serdes.Long(), Serdes.String(), "ml-movies-rating-in");

        //read the movie rating back as a globalKTable
        GlobalKTable<Long, String> movieRatingKTable = kStreamBuilder.globalTable(
                Serdes.Long(), Serdes.String(), "ml-movies-rating-in");

        //stream the ratings
        KStream<Long, Rating> ratingStream = kStreamBuilder
                .stream(Serdes.String(), Serdes.String(), "ml-ratings-in").filter(new Predicate<String, String>() {
            @Override
            public boolean test(String k, String v) {
                return !v.contains("userId"); //the header
            }
        }).map(new KeyValueMapper<String, String, KeyValue<Long, Rating>>() {
            @Override
            public KeyValue<Long, Rating> apply(String k, String v) {
                Rating rating = Rating.fromString(v);
                return new KeyValue<>(rating.getMovieId(), rating);
            }
        });

        KStream<Long, String> joinedStream = ratingStream.join(movieRatingKTable,
                new KeyValueMapper<Long, Rating, Long>() {
            @Override
            public Long apply(Long k, Rating v) {
                return k;
            }
        }, new ValueJoiner<Rating, String, String>() {
            @Override
            public String apply(Rating rating, String v2) {
                MovieRating movieRating = MovieRating.fromString(v2).addRating(rating);
                return movieRating.toString();
            }
        });

        //write out this new value to output
        joinedStream.to(Serdes.Long(), Serdes.String(), "ml-denor-movie-rating");

        final KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, prop);
        kafkaStreams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
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
