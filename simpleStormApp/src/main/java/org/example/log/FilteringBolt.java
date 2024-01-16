package org.example.log;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilteringBolt extends BaseBasicBolt {
    String notificationFilePath = "/storm/labs/notifications.log";
    int numOfTooManyAuth = 0;

    class ParsedLog {
        boolean isValid = false;
        String username;
        String ip;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String log = tuple.getStringByField("line");

        ParsedLog parsedLog = parseLog(log);

        if (parsedLog.isValid){
            System.out.println("\u001B[32mEmitting " + log + "\u001B[0m");
            basicOutputCollector.emit("attempt", new Values(1));
            basicOutputCollector.emit("username", new Values(parsedLog.username));
            basicOutputCollector.emit("ip", new Values(parsedLog.ip));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("attempt", new Fields("entier"));
        outputFieldsDeclarer.declareStream("username", new Fields("name"));
        outputFieldsDeclarer.declareStream("ip", new Fields("address"));
    }

    private ParsedLog parseLog(String log) {
        ParsedLog parsedLog = new ParsedLog();

        Pattern pattern = Pattern.compile("\\w+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\w+\\s+sshd\\[\\d+]:\\s+\\w+\\spassword for(\\sinvalid user)?\\s+(\\w+)\\s+from\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+port\\s+\\d+\\s+ssh2");
        Matcher m = pattern.matcher(log);
        if (m.find()) {
            parsedLog.username = m.group(2);
            parsedLog.ip = m.group(3);
            parsedLog.isValid = true;
        } else {
            Pattern patternTooManyAuth = Pattern.compile("\\w+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\w+\\s+sshd.*Too many authentication failures for.*");
            Matcher mToManyAuth = patternTooManyAuth.matcher(log);
            if (mToManyAuth.find()) {
                numOfTooManyAuth++;
                sendNotification("Number of too many authentication failures : " + numOfTooManyAuth);
            }
        }
        return parsedLog;
    }

    private void sendNotification(String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(notificationFilePath, true))) {
            // message prefix with date
            String messagePrefix = java.time.LocalDateTime.now() + " : ";
            writer.write(messagePrefix + message);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();  // Handle the exception according to your needs
        }
    }
}
