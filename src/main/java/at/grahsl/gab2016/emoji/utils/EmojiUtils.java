package at.grahsl.gab2016.emoji.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.vdurmont.emoji.EmojiManager;

public class EmojiUtils {

	public static final String EMOJIS_PATTERN;
	public static final Pattern REGEXP_PATTERN;
	public static final Set<String> EMOJIS_TO_FILTER = 
			EmojiManager.getAll().stream()
				.map(e -> e.getUnicode())
				.collect(Collectors.toSet());

	static {
		// since unicode ranges of emojis are not continuous
		// we simply compile a pattern which contains
		// every single emoji the library currently knows
		String pattern = 
				EmojiManager.getAll().stream()
				.map(e -> e.getUnicode())
				.sorted((s1,s2) -> Integer.valueOf(s2.length()).compareTo(s1.length()))
				.collect(Collectors.joining("|"));
		
		//replace potential meta characters
		EMOJIS_PATTERN = pattern.replaceAll("\\*","");
		REGEXP_PATTERN = Pattern.compile(EMOJIS_PATTERN);
	}

	public static List<String> extractEmojisAsString(String original) {

		Matcher matcher = REGEXP_PATTERN.matcher(original);
		List<String> matchList = new ArrayList<String>();
		while (matcher.find()) {
			String m = matcher.group();
			if(EMOJIS_TO_FILTER.contains(m)) {
				matchList.add(matcher.group());
			}
		}
		return matchList;

	}
	
}
