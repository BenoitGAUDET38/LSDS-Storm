package org.example.simple;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    Random random;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        random = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        int randomInt = random.nextInt(1000);
        System.out.println("Emitting " + randomInt);
        collector.emit(new Values(randomInt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("entier"));
    }
}
