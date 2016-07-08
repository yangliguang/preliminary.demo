package com.alibaba.middleware.race.jstorm.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;


/**
 * 类说明
 * @Author yangliguang
 * 2016年7月6日上午11:26:41
 */
public class WordCountLocal implements IRichBolt {
    OutputCollector collector;
    Map<String, Integer> counts = new HashMap<String, Integer>();

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        counts.put(word, ++count);
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    public void cleanup() {
        // TODO Auto-generated method stub

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
