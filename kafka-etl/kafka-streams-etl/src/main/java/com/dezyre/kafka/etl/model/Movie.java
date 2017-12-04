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
public class Movie implements Serializable {

    private Long movieId;
    private String title;
    private String year;
    private String genres;

    /**
     *
     */
    public Movie() {
        this(null, "", "", "");
    }

    /**
     *
     * @param movieId
     * @param title
     * @param year
     * @param genres
     */
    public Movie(Long movieId, String title, String year, String genres) {
        this.movieId = movieId;
        this.title = title;
        this.year = year;
        this.genres = genres;
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

    @Override
    public String toString() {
        if (!this.year.isEmpty()) {
            return String.format("%d,%s(%s),%s", this.movieId, this.title, this.year, this.genres);
        } else {
            return String.format("%d,%s,,%s", this.movieId, this.title, this.genres);
        }
    }

    public static Movie fromString(String value) {
        String[] parts = value.replace("\"", "").split(",");
        Movie movie = new Movie();
        if (parts.length == 3) {
            movie.movieId = Long.parseLong(parts[0]);
            movie.genres = detectNoGenre(parts[2]);
            getYearTitle(parts[1], movie);
        } else if (parts.length > 3) {
            movie.movieId = Long.parseLong(parts[0]);
            StringBuilder sb = new StringBuilder("");
            for (int i = 1; i < parts.length - 1; i++) {
                sb.append(", ").append(parts[i]);
            }
            getYearTitle(sb.toString(), movie);
            movie.genres = detectNoGenre(parts[parts.length - 1]);
        } else {
            //do nothing
        }

        return movie;
    }

    private static void getYearTitle(String s, Movie movie) {
        String[] titleYear = s.replace(")", "").replace("(", "@").split("@");
        movie.title = titleYear[0];
        if (titleYear.length > 1) {
            movie.year = titleYear[1];
        }
    }

    private static String detectNoGenre(String s) {
        if (s.contains("listed")) {
            return "-";
        } else {
            return s;
        }
    }

//    public static void main(String[] args) {
//        String test1 = "176177,Antidur (2007),Action|Comedy|Crime";
//        String test2 = "176181,The Fortunes and Misfortunes of Moll Flanders (1996),Comedy|Drama|Romance";
//        String test3 = "176183,The Prisoner of If Castle (1988),Adventure";
//        String test4 = "176185,\"A Black Rose Is an Emblem of Sorrow, a Red Rose an Emblem of Love (1989)\",Comedy|Drama|Fantasy|Romance";
//
//        String test5 = "176269,Subdue,Children|Drama";
//        String test6 = "176271,Century of Birthing (2011),Drama";
//        String test7 = "176273,Betrayal (2003),Action|Drama|Thriller";
//        String test8 = "176275,Satan Triumphant (1917),(no genres listed)";
//        String test9 = "176279,Queerama (2017),(no genres listed)";
//
//        System.out.println(Movie.fromString(test1));
//        System.out.println(Movie.fromString(test2));
//        System.out.println(Movie.fromString(test3));
//        System.out.println(Movie.fromString(test4));
//        System.out.println(Movie.fromString(test5));
//        System.out.println(Movie.fromString(test6));
//        System.out.println(Movie.fromString(test7));
//        System.out.println(Movie.fromString(test8));
//        System.out.println(Movie.fromString(test9));
//
//    }
}
