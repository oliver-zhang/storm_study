package com.oliver.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**   
 * Copyright: Copyright (c) 2012 Asiainfo
 * 
 * @ClassName: DoubleAndTripleBolt.java
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
public class DoubleAndTripleBolt extends BaseRichBolt {
	
	private OutputCollector _collector;

	public void execute(Tuple tuple) {
		int val = tuple.getInteger(0);        
        _collector.emit(tuple, new Values(val*2, val*3));
        _collector.ack(tuple);
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("double", "triple"));
	}

}
