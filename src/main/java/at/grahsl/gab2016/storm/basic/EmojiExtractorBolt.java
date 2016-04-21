package at.grahsl.gab2016.storm.basic;

import at.grahsl.gab2016.emoji.utils.EmojiUtils;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmojiExtractorBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 3540675814892197241L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String tweetMsg = input.getStringByField("tweetMsg");
		EmojiUtils.extractEmojisAsString(tweetMsg).stream()
				.forEach(e -> {collector.emit(new Values(e));});
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("emoji"));
	}

}
