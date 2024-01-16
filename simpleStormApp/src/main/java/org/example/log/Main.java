package org.example.log;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class Main {

    public static void main(String[] args) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new LogSpout());

        builder.setBolt("filter", new FilteringBolt()).shuffleGrouping("spout");

        // We only want the tuples from the attempt stream
        builder.setBolt("attempt", new AttemptBolt().withTumblingWindow(
                new BaseWindowedBolt.Count(5))).shuffleGrouping("filter", "attempt");

        builder.setBolt("username", new UsernameBolt().withTumblingWindow(
                new BaseWindowedBolt.Count(5))).shuffleGrouping("filter", "username");

        builder.setBolt("ip", new IpBolt().withTumblingWindow(
                new BaseWindowedBolt.Count(5))).shuffleGrouping("filter", "ip");

        Config conf = new Config();

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
}
