package org.example.simple;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class FilteringBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//        Utils.sleep(100);
        Integer randomInt = tuple.getIntegerByField("entier");
        if (randomInt % 2 != 0) {
            System.out.println("Filter emit " + randomInt);
            basicOutputCollector.emit(new Values(randomInt));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("entier"));
    }
}
