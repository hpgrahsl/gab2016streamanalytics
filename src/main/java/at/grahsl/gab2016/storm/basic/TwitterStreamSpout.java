package at.grahsl.gab2016.storm.basic;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import at.grahsl.gab2016.storm.common.TweetsListener;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterStreamSpout extends BaseRichSpout {

	private static final long serialVersionUID = -2756121294671222195L;
	
	private SpoutOutputCollector collector;
	private BlockingQueue<Status> tweetsQueue = new LinkedBlockingQueue<>(4096);
	private TwitterStream stream;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
        TwitterStreamFactory tsFactory = new TwitterStreamFactory();
		stream = tsFactory.getInstance();
		stream.addListener(new TweetsListener(tweetsQueue));
		stream.sample();
	}
	
	@Override
	public void nextTuple() {
		try {
			Status next = tweetsQueue.take();
			collector.emit(new Values(next.getText()));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void ack(Object id) {
		//TODO: not used/needed for this demo
	}

	@Override
	public void fail(Object id) {
		//TODO: not used/needed for this demo
	}
	
	@Override
	public void close() {
		stream.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweetMsg"));
	}

}
