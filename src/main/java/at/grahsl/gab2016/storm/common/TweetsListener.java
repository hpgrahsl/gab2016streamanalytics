package at.grahsl.gab2016.storm.common;

import java.util.concurrent.BlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TweetsListener implements StatusListener {
	
	private BlockingQueue<Status> queue;
	
	public TweetsListener(BlockingQueue<Status> queue) {
		this.queue = queue;
	}
	
	@Override
    public void onStatus(Status status) {
		queue.offer(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {}

    @Override
    public void onStallWarning(StallWarning warning) {}

    @Override
    public void onException(Exception exc) {
        exc.printStackTrace();
    }

}
