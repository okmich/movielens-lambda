/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafka.etl;

import com.dezyre.kafka.etl.model.MovieRating;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 *
 * @author m.enudi
 */
public class TopNRatedMovieStreamApp {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put("application.id", "TopNRatedMovieStreamApp");
        prop.put("bootstrap.servers", "localhost:9092");

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KTable<Long, String> movieRatingTable
                = kStreamBuilder.table(Serdes.Long(), Serdes.String(), "ml-denor-movie-rating");

        KTable<Long, String> resultTable
                = movieRatingTable.groupBy(
                        new KeyValueMapper<Long, String, KeyValue<Long, MovieRating>>() {
                    @Override
                    public KeyValue<Long, MovieRating> apply(Long k, String v) {
                        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                    }
                }).reduce(new MovieRatingReducer(), new MovieRatingReducer())
                        .mapValues(new ValueMapper<MovieRating, String>() {
                            @Override
                            public String apply(MovieRating v) {
                                return v.toString();
                            }
                        });

    }

    private static class MovieRatingReducer implements Reducer<MovieRating> {

        @Override
        public MovieRating apply(MovieRating v, MovieRating v1) {
            return v.addRating(v1, true);
        }

    }

}
