package org.example.simple;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class AveragingBolt extends BaseBasicBolt {
    int count = 0;
    int totalLast10Int = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Integer randomInt = tuple.getIntegerByField("entier");
        totalLast10Int += randomInt;
        count++;

        if (count == 10) {
            System.out.println("Average emit " + totalLast10Int / 10);
            totalLast10Int = 0;
            count = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("entier"));
    }
}
