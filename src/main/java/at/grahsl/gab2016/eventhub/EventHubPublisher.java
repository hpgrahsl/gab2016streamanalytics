package at.grahsl.gab2016.eventhub;

import java.io.IOException;
import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class EventHubPublisher {

	public static void main(String[] args) throws NamingException, JMSException, IOException, InterruptedException {
		
		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,
				"org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
		env.put(Context.PROVIDER_URL, "src/main/resources/servicebus.properties");
		Context context = new InitialContext(env);

		ConnectionFactory cf = (ConnectionFactory) context.lookup("SBCF");

		Destination queue = (Destination) context.lookup("EventHub");

		Connection connection = cf.createConnection();

		Session sendSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer sender = sendSession.createProducer(queue);
		
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();        
        twitterStream.addListener(new TweetsListener(sendSession, sender));
        
        // sample() method internally creates a thread which manipulates
        // TwitterStream and calls these adequate listener methods continuously.
        twitterStream.sample();
        
	}
	
}
