/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.movielens.kafkaproducer.mock;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m.enudi
 */
public class Main {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("USAGE: <file_path> <kafka_broker_url> <topic_name>");
            System.exit(-1);
        }
        try {
//            new ClientSimulator("F:\\data_dump\\movielens\\ml-latest\\ratings.csv", "", "").start();
            new ClientSimulator(args[0], args[1], args[2]).start();
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
