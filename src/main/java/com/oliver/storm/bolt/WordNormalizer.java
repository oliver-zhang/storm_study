package com.oliver.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**   
 * Copyright: Copyright (c) 2012 Asiainfo
 * 
 * @ClassName: WordNormalizer.java
 * @Description: 
 *
 * @version: v1.0.0
 * @author: zhangjian
 * @date: 2016年3月3日 
 *
 * Modification History:
 * Date         Author          Version            Description
 *---------------------------------------------------------*
 * 2016年3月3日    zhangjian           v1.0.0               修改原因
 */
public class WordNormalizer extends BaseBasicBolt {

	public void cleanup() {}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);  
        String[] words = sentence.split(" ");  
        for(String word : words){  
            word = word.trim();  
            if(!word.isEmpty()){  
                word = word.toLowerCase();  
                collector.emit(new Values(word));  
            }  
        }  
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
