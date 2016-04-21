package at.grahsl.gab2016.storm.basic;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class EmojiTrackerBasicTopo {

	public final static String TOPOLOGY_NAME = "EmojiTrackerBasic";
	public final static String TWEET_SPOUT = "tweet_stream";
	public final static String EXTRACTOR_BOLT = "emoji_extractor";
	public final static String COUNTER_BOLT = "total_counter";
	public final static String PERSISTENCE_BOLT = "db_persister";
	
	public static void main(String[] args) {
		
		if(args.length != 1 
				|| (!"local".equals(args[0]) 
						&& !"cluster".equals(args[0]) )) {
			System.err.println("error: unknown program args - use either [local | cluster]");
			System.exit(-1);
		}
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(TWEET_SPOUT, new TwitterStreamSpout());

		builder.setBolt(EXTRACTOR_BOLT, new EmojiExtractorBolt(),4)
					.shuffleGrouping(TWEET_SPOUT);
		
		builder.setBolt(COUNTER_BOLT, new CounterBolt(),4)
					.fieldsGrouping(EXTRACTOR_BOLT, new Fields("emoji"));		

		builder.setBolt(PERSISTENCE_BOLT, new PersistenceBolt(),4)
					.shuffleGrouping(COUNTER_BOLT);
		
		Config conf = new Config();
		
		if("local".equals(args[0])) {
			conf.setNumWorkers(1);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			
			Utils.sleep(300_000);
			
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		} 
		
		if ("cluster".equals(args[0])) {
			conf.setNumWorkers(2);
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
		
		
	}

}
