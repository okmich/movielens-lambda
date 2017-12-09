package com.dezyre.kafka.etl.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SummaryRating implements java.io.Serializable {

	private Long movieId;
	private String year;
	private String title;
	private int ratingCount;
	private double ratingTotal;

	public SummaryRating() {
	}

	public Long getMovieId() {
		return movieId;
	}

	public void setMovieId(Long movieId) {
		this.movieId = movieId;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public int getRatingCount() {
		return ratingCount;
	}

	public void setRatingCount(int ratingCount) {
		this.ratingCount = ratingCount;
	}

	public double getRatingTotal() {
		return ratingTotal;
	}

	public void setRatingTotal(double ratingTotal) {
		this.ratingTotal = ratingTotal;
	}

	public JsonNode toJsonNode() {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.valueToTree(this);
	}

	public static SummaryRating fromJsonNode(JsonNode jsonNode) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.treeToValue(jsonNode, SummaryRating.class);
		} catch (Exception e) {
			e.printStackTrace();
			return new SummaryRating();
		}
	}

	public SummaryRating mergeSummaries(SummaryRating v1) {
		this.ratingCount += v1.ratingCount;
		this.ratingTotal += v1.ratingTotal;
		this.movieId = v1.movieId == null ? this.movieId : v1.movieId;
		this.title = v1.title == null ? this.title : v1.title;
		this.year = v1.year == null ? this.year : v1.year;

		return this;
	}
}
