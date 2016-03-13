package com.oliver.storm.topology;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**   
 * Copyright: Copyright (c) 2012 Asiainfo
 * 
 * @ClassName: TranctionTopology.java
 * @Description: 
 *
 * @version: v1.0.0
 * @author: zhangjian
 * @date: 2016年3月11日 
 *
 * Modification History:
 * Date         Author          Version            Description
 *---------------------------------------------------------*
 * 2016年3月11日    zhangjian           v1.0.0               修改原因
 */
public class TranctionTopology {

	public static void main(String[] args) throws Exception {
		
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
	               new Values("the cow jumped over the moon"),
	               new Values("the man went to the store and bought some candy"),
	               new Values("four score and seven years ago"),
	               new Values("how many apples can you eat"));
		spout.setCycle(true);
		
		
		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology.newStream("spout1", spout).each(new Fields("sentence"), new Split(), new Fields("word"))
			       .groupBy(new Fields("word"))
			       .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))                
			       .parallelismHint(6);
		
		
	}
}
