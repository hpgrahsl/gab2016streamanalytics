package at.grahsl.gab2016.storm.trident;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import com.vdurmont.emoji.EmojiManager;
import com.vdurmont.emoji.EmojiParser;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class EmojiExtractorFunction extends BaseFunction {

	private static final long serialVersionUID = 2425918756470493478L;
	
	private static Set<String> EMOJI_SET;

	static {
		EMOJI_SET = EmojiManager.getAll().stream()
				.map(e -> e.getUnicode())
				.collect(Collectors.toSet());
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		final String tweetMsg = tuple.getStringByField("tweetMsg");
		Arrays.asList(tweetMsg.split("\\s+")).stream()
				.filter(w -> EMOJI_SET.contains(w)).forEach(e -> {
						collector.emit(new Values(EmojiParser.parseToHtmlHexadecimal(e)));
		});

	}
}
