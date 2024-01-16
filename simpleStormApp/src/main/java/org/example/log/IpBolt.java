package org.example.log;

import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class IpBolt extends BaseWindowedBolt {
    Map<String, Integer> mapIp = new HashMap<>();

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()) {
            String ip = tuple.getStringByField("address");
            if (mapIp.containsKey(ip)) {
                mapIp.put(ip, mapIp.get(ip) + 1);
            } else {
                mapIp.put(ip, 1);
            }
        }
        StringBuilder output = new StringBuilder("\u001B[35m");
        output.append("\nNumber of login attempts per IP :\n");
        for (Map.Entry<String, Integer> entry : mapIp.entrySet()) {
            output.append("   ").append(entry.getKey()).append(" try logging ").append(entry.getValue()).append(" times\n");
        }
        output.append("\u001B[0m");
        System.out.println(output);
    }
}
