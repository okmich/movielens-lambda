/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.movielens.kafkaproducer.mock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m.enudi
 */
public class ClientSimulator {

    private KafkaProducerInterface kafkaProducerInterface;
    private BufferedReader bufferedFileReader;

    public ClientSimulator(String file, String brokerUrl, String topic) {
        File _file = new File(file);
        if (!_file.exists()) {
            throw new IllegalArgumentException(file + " does not exist");
        }
        try {
            this.bufferedFileReader = new BufferedReader(new FileReader(_file));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ClientSimulator.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public void start() throws IOException {
        int count = 0;
        String line = this.bufferedFileReader.readLine();
        while (line == null) {
            kafkaProducerInterface.send(line);
            count++;
            //flush after a certain number of send message
            if (count == 2000) {
                kafkaProducerInterface.flush();
                count = 0;
            }
        }
        kafkaProducerInterface.flush();
        kafkaProducerInterface.close();

    }

}
