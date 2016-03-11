package com.oliver.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.oliver.storm.bolt.WordCounter;
import com.oliver.storm.bolt.WordNormalizer;
import com.oliver.storm.spout.WordReader;


/**   
 * Copyright: Copyright (c) 2012 Asiainfo
 * 
 * @ClassName: WordCountTopology.java
 * @Description: 
 *
 * @version: v1.0.0
 * @author: zhangjian
 * @date: 2016年3月2日 
 *
 * Modification History:
 * Date         Author          Version            Description
 *---------------------------------------------------------*
 * 2016年3月2日    zhangjian           v1.0.0               修改原因
 */
public class WordCountTopology {

	 public static void main(String[] args) throws Exception {

		    TopologyBuilder builder = new TopologyBuilder();

		    builder.setSpout("word-reader", new WordReader(), 5);
		    builder.setBolt("word-normalizer", new WordNormalizer(), 8).shuffleGrouping("word-reader");
		    builder.setBolt("word-counter", new WordCounter(), 12).fieldsGrouping("word-normalizer", new Fields("word"));

		    Config conf = new Config();
		    conf.setDebug(true);
	        conf.setMaxTaskParallelism(1);
	        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	        conf.put("wordsFile", "D://tmp/storm_words.txt");
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("word-count", conf, builder.createTopology());
	        Thread.sleep(10000);
	        cluster.shutdown();
	        
	 }
}
