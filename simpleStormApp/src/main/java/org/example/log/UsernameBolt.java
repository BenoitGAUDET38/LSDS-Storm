package org.example.log;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class UsernameBolt extends BaseWindowedBolt {
    Map<String, Integer> mapUsername = new HashMap<>();

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()) {
            String ip = tuple.getStringByField("name");
            if (mapUsername.containsKey(ip)) {
                mapUsername.put(ip, mapUsername.get(ip) + 1);
            } else {
                mapUsername.put(ip, 1);
            }
        }
        StringBuilder output = new StringBuilder("\u001B[31m");
        output.append("\nUsername popularity :\n");
        for (Map.Entry<String, Integer> entry : mapUsername.entrySet()) {
            output.append("   ").append(entry.getKey()).append(" is used ").append(entry.getValue()).append(" times\n");
        }
        output.append("\u001B[0m");
        System.out.println(output);
    }
}
