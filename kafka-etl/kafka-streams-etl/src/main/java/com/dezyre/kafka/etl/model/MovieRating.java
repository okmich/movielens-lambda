/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafka.etl.model;

import java.io.Serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author m.enudi
 */
public class MovieRating implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long movieId;
	private String title;
	private String year;
	private String genres;
	private Long userId;
	private Float rating;
	private Long timestamp;

	public MovieRating() {
	}

	/**
	 * @return the movieId
	 */
	public Long getMovieId() {
		return movieId;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @return the year
	 */
	public String getYear() {
		return year;
	}

	/**
	 * @return the genres
	 */
	public String getGenres() {
		return genres;
	}

	/**
	 * @return the userId
	 */
	public Long getUserId() {
		return userId;
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
		return String.format("%d##%d##%s##%s##%8.2f##%d##%s", this.movieId,
				this.userId, this.title, this.year, this.rating,
				this.timestamp, this.genres);
	}

	public JsonNode toJsonNode() {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.valueToTree(this);
	}

	public static MovieRating fromString(String record) {
		String[] parts = record.split("##");

		MovieRating mr = new MovieRating();

		mr.genres = parts[6];
		mr.movieId = Long.parseLong(parts[0]);
		mr.rating = Float.parseFloat(parts[4]);
		mr.timestamp = Long.parseLong(parts[5]);
		mr.title = parts[2];
		mr.userId = Long.parseLong(parts[1]);
		mr.year = parts[3];

		return mr;
	}

	public static MovieRating fromJsonNode(JsonNode jsonNode) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.treeToValue(jsonNode, MovieRating.class);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return new MovieRating();
		}
	}

	public static MovieRating fromMovieString(String value) {
		Movie movie = Movie.fromString(value);
		MovieRating mr = new MovieRating();

		mr.genres = movie.getGenres();
		mr.movieId = movie.getMovieId();
		mr.rating = 0f;
		mr.timestamp = 0l;
		mr.title = movie.getTitle();
		mr.userId = 0l;
		mr.year = movie.getYear();

		return mr;
	}

	public MovieRating addRating(Rating rating) {
		this.timestamp = rating.getTimestamp();
		this.rating = rating.getRating();
		this.userId = rating.getUserId();

		return this;
	}

	public MovieRating addRating(MovieRating v1, boolean takeLatest) {
		this.rating += v1.rating;
		if (takeLatest) {
			this.timestamp = (this.timestamp > v1.timestamp ? this.timestamp
					: v1.timestamp);
		} else {
			this.timestamp = (this.timestamp < v1.timestamp ? this.timestamp
					: v1.timestamp);
		}
		this.userId = 0l;

		return this;
	}

}
