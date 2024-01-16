package org.example.log;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class AttemptBolt extends BaseWindowedBolt {
    int numOfAttempt = 0;

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()) {
            numOfAttempt++;
        }
        System.out.println("\u001B[34mNumber total of login attempts : " + numOfAttempt + "\u001B[0m");
    }
}
