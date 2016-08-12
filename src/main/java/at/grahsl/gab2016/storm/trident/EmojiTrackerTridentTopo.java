package at.grahsl.gab2016.storm.trident;

import org.apache.spark.streaming.Durations;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.MemoryMapState;

import at.grahsl.gab2016.storm.common.MyJdbcUtils;


public class EmojiTrackerTridentTopo {

	public final static String TOPOLOGY_NAME = "EmojiTrackerTrident";
	
	public static void main(String[] args) {

		if(args.length != 1 
				|| (!"local".equals(args[0]) 
						&& !"cluster".equals(args[0]) )) {
			System.err.println("error: unknown program args - use either [local | cluster]");
			System.exit(-1);
		}
		
		final TridentTopology topology = new TridentTopology();
			
		//DB persisted emoji counting
		topology.newStream("twitterStream", new TwitterStreamBatchSpout(128))
			.each(new Fields("tweetMsg"), new EmojiExtractorFunction(), new Fields("ecode"))
			.groupBy(new Fields("ecode"))
			.aggregate(new Count(), new Fields("counter"))
			.parallelismHint(4)
			.each(new Fields("ecode", "counter"), new Debug("BATCHED EMOJI MAP"))
			.partitionPersist(MyJdbcUtils.getJdbcFactory(),
					new Fields("ecode","counter"),  new JdbcUpdater(), new Fields("emoji","freq"));
			
		//IN-MEMORY persisted emoji counting
//		topology.newStream("twitterStream", new TwitterStreamBatchSpout(512))
//				.each(new Fields("tweetMsg"), new EmojiExtractorFunction(), new Fields("emoji"))
//				.groupBy(new Fields("emoji"))
//				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("counter"))
//				.parallelismHint(4)
//				.newValuesStream()
//				.each(new Fields("emoji", "counter"), new Debug("BATCHED EMOJI MAP"));
				
		Config conf = new Config();
		
		if("local".equals(args[0])) {
			conf.setNumWorkers(1);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, conf, topology.build());
			
			//! FOR DEMO PURPOSES ONLY !
	        //stops the topology after certain
	        //time period e.g. 10 minutes
			Utils.sleep(Durations.minutes(10).milliseconds());
			
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		} 
		
		if ("cluster".equals(args[0])) {
			conf.setNumWorkers(2);
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, topology.build());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
		
	}

}