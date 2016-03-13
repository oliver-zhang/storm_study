package com.oliver.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**   
 * Copyright: Copyright (c) 2012 Asiainfo
 * 
 * @ClassName: WordCounter.java
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
public class WordCounter extends BaseBasicBolt {
	
	Integer id;  
    String name;  
    Map<String, Integer> counters;
    
    /**
     * On destory
     */
    @Override  
    public void cleanup() {  
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");  
        for(Map.Entry<String, Integer> entry : counters.entrySet()){  
            System.out.println(entry.getKey()+": "+entry.getValue());  
        }  
    }  
    
    /** 
     * On create  
     */  
    @Override  
    public void prepare(Map stormConf, TopologyContext context) {  
        this.counters = new HashMap<String, Integer>();  
        this.name = context.getThisComponentId();  
        this.id = context.getThisTaskId();  
    } 

	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);  
        if(!counters.containsKey(str)){  
            counters.put(str, 1);  
        }else{  
            Integer c = counters.get(str) + 1;  
            counters.put(str, c);  
        }  
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
