package at.grahsl.gab2016.eventhub;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.commons.compress.utils.CharsetNames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vdurmont.emoji.EmojiParser;

import at.grahsl.gab2016.emoji.utils.EmojiUtils;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TweetsListener implements StatusListener {

	private final Session session;
	private final MessageProducer producer;
	private final ObjectMapper mapper = new ObjectMapper();
	
	public TweetsListener(Session session,MessageProducer producer) {
		this.session = session;
		this.producer = producer;
	}
	
	@Override
    public void onStatus(Status status) {
    	try {
    		List<String> emojis = EmojiUtils.extractEmojisAsString(status.getText())
    				.stream()
    				.map(w -> EmojiParser.parseToHtmlHexadecimal(w).replaceAll("&#x", ""))
					.collect(Collectors.toList());
    		
    		if(emojis.size() > 0) {
    			String raw = mapper.writeValueAsString(
    					new MessageEH(status.getCreatedAt(), status.getId(),
    										emojis, status.getText()));
    			BytesMessage message = session.createBytesMessage();
    			message.writeBytes(raw.getBytes(CharsetNames.UTF_8));
    			System.out.println(raw);
    			producer.send(message);
    		}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.err.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        System.err.println("Got stall warning:" + warning);
    }

    @Override
    public void onException(Exception ex) {
        ex.printStackTrace();
    }

}
