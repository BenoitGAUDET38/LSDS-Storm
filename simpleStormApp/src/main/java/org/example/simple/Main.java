package org.example.simple;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.example.simple.AveragingWindowBolt;
import org.example.simple.FilteringBolt;
import org.example.simple.RandomSpout;

public class Main {
    public static void main(String[] args) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("random-spout", new RandomSpout(), 4)
//                .setNumTasks(4);
        builder.setSpout("random-spout", new RandomSpout());
        builder.setBolt("filter-odd", new FilteringBolt())
                .shuffleGrouping("random-spout");
//        builder.setBolt("average", new AveragingBolt())
//                .shuffleGrouping("filter-odd");
        builder.setBolt("average-window", new AveragingWindowBolt().withTumblingWindow(new BaseWindowedBolt.Count(10)))
                .shuffleGrouping("filter-odd");

        Config config = new Config();
//        config.setNumWorkers(2);
        StormSubmitter.submitTopologyWithProgressBar("simple-storm", config, builder.createTopology());

    }
}