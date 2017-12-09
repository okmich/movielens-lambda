/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafka.etl.model;

import java.io.Serializable;

/**
 *
 * @author m.enudi
 */
public class Rating implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long userId;
    private Long movieId;
    private Float rating;
    private Long timestamp;

    /**
     *
     * @param userId
     * @param movieId
     * @param rating
     * @param timestamp
     */
    public Rating(Long userId, Long movieId, Float rating, Long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    /**
     * @return the userId
     */
    public Long getUserId() {
        return userId;
    }

    /**
     * @return the movieId
     */
    public Long getMovieId() {
        return movieId;
    }

    /**
     * @return the rating
     */
    public Float getRating() {
        return rating;
    }

    /**
     * @return the timestamp
     */
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%8.2f,%d", this.userId, this.movieId, this.rating, this.timestamp);
    }

    /**
     *
     * @param value
     * @return
     */
    public static Rating fromString(String value) {
        String[] parts = value.replace("\"", "").split(",");
        Rating rating = new Rating(
                Long.parseLong(parts[0]),
                Long.parseLong(parts[1]),
                Float.parseFloat(parts[2]),
                Long.parseLong(parts[3])
        );
        return rating;
    }

}
