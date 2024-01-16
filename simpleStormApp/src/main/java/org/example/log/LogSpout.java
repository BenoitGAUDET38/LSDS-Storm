package org.example.log;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class LogSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final String filePath = "/storm/labs/SSH.log";
    private BufferedReader bufferedReader;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            // Open the file for reading
            FileReader fileReader = new FileReader(filePath);
            bufferedReader = new BufferedReader(fileReader);
        } catch (IOException e) {
            throw new RuntimeException("Error opening file: " + filePath, e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = bufferedReader.readLine();

            if (line != null) {
                System.out.println("\u001B[33mEmitting " + line + "\u001B[0m");
                collector.emit(new Values(line));
                Thread.sleep(100);
            } else {
                System.out.println("No line left");
                Thread.sleep(100);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error reading from file: " + filePath, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public void close() {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error closing file: " + filePath, e);
        }
    }
}
