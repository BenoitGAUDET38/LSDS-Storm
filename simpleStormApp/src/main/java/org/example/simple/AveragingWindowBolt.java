package org.example.simple;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class AveragingWindowBolt extends BaseWindowedBolt {
    @Override
    public void execute(TupleWindow tupleWindow) {
        int total = 0;
        for (Tuple tuple : tupleWindow.get()) {
            total += tuple.getIntegerByField("entier");
        }
        System.out.println("Average window emit " + total / tupleWindow.get().size());
    }
}
