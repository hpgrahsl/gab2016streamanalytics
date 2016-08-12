package at.grahsl.gab2016.storm.trident;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import at.grahsl.gab2016.storm.common.TweetsListener;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

@SuppressWarnings("rawtypes")
public class TwitterStreamBatchSpout implements IBatchSpout {

	private static final long serialVersionUID = 2116832608633626133L;
	
	private BlockingQueue<Status> tweetsQueue = new LinkedBlockingQueue<>(8192);
	private TwitterStream stream;
	private int batchSize;
	
	public TwitterStreamBatchSpout(int batchSize) {
		this.batchSize = batchSize;
	}
	
	@Override
	public void open(Map conf, TopologyContext context) {
		TwitterStreamFactory tsFactory = new TwitterStreamFactory();
		stream = tsFactory.getInstance();
		stream.addListener(new TweetsListener(tweetsQueue));
		stream.sample();
	}
	
	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		for(int t = 0;t<batchSize; t++) {
			try {
				Status next = tweetsQueue.take();
				collector.emit(new Values(next.getText()));
			} catch (InterruptedException exc) {
				exc.printStackTrace();
			}
		}
	}
	
	@Override
	public void ack(long batchId) {
		//TODO: not used/needed for this demo
	}
	
	@Override
	public void close() {
		stream.shutdown();
	}
	
	@Override
	public Map getComponentConfiguration() {
		return new Config();
	}
	
	@Override
	public Fields getOutputFields() {
		return new Fields("tweetMsg");
	}
	
}
