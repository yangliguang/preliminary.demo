package com.alibaba.middleware.race.jstorm.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.jstorm.RaceSentenceSpout;
import com.alibaba.middleware.race.jstorm.SplitSentence;
import com.alibaba.middleware.race.jstorm.WordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 类说明
 * @Author yangliguang
 * 2016年7月5日下午5:57:35
 */
public class RaceTopologyLocal {
	private static Logger log = LoggerFactory.getLogger(RaceTopologyLocal.class);

	public static void main(String[] args) {
		LocalCluster cluster = new LocalCluster();

		
		/* begin young-define*/
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new SpoutLocal(), 1);
        builder.setBolt("split", new SplitSentenceLocal(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountLocal(), 1).fieldsGrouping("split", new Fields("word"));
        /* end young-define */
        
		
		//建议加上这行，使得每个bolt/spout的并发度都为1
		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);

		//提交拓扑
		cluster.submitTopology("SequenceTest", conf, builder.createTopology());

		//等待1分钟， 1分钟后会停止拓扑和集群， 视调试情况可增大该数值
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        

		//结束拓扑
		cluster.killTopology("SequenceTest");

		cluster.shutdown();
	}
	
}
