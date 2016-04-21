package at.grahsl.gab2016.storm.basic;

import java.util.HashMap;
import java.util.Map;

import com.vdurmont.emoji.EmojiParser;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CounterBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 3893015458842308959L;
	
	private Map<String, Long> freqMap = new HashMap<>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String emoji = tuple.getStringByField("emoji");
		Long freq = freqMap.get(emoji);
		if (freq == null)
			freq = 0L;
		freq++;
		freqMap.put(emoji, freq);
		collector.emit(new Values(EmojiParser.parseToHtmlHexadecimal(emoji),1L));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ecode", "counter"));
	}
	
	@Override
	public void cleanup() {
		//for demo purpose only
		System.out.println("printing intermediate map:");
		for(Map.Entry<String, Long> entry:freqMap.entrySet()){
			System.out.println(entry.getKey()+" => " + entry.getValue());
		}
		System.out.println("#entries: "+freqMap.size());
	}
	
}
