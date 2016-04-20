package at.grahsl.gab2016.spark.stream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.vdurmont.emoji.EmojiParser;

import at.grahsl.gab2016.emoji.utils.EmojiUtils;
import scala.Tuple2;

public class EmojiTrackerDB {
	
	//relative to the running user and defaults to wasb:/// schema
	//which writes to Azure Blob Storage when run on HDInsight cluster
	public static final String CHECKPOINT_DIR = "data/checkpoint/";
	public static final String OUTPUT_DIR = "data/output/streaming/";
	
	//MYSQL VERSION
	//TODO: add your jdbc connection string here
	public static final String JDBC_CONNECTION = "YOUR_JDBC_CONNECTION_STRING_TO_MYSQL";
    
	//MYSQL insert stmt to achieve upsert semantic in order to
	//be keep an ever increasing running total while the job is running
	//TODO: write an upsert statement for your database system here	
	public static final String SQL_UPSERT_EMOJI_COUNT = "INSERT INTO emoji_freqs_spark (ecode,counter) VALUES (?,?) ON DUPLICATE KEY UPDATE counter=counter+?";

	//AZURE SQL DB VERSION
	//public static final String JDBC_CONNECTION = "YOUR_JDBC_CONNECTION_STRING_TO_AZURE_SQL_DB";
	
    //the following just performs an UPSERT which is
	//somehow quite complicated on sql server :) we
	//might be better of and go for an insert trigger
	/*
	 public static final String SQL_UPSERT_EMOJI_COUNT = 
			"WITH src AS (SELECT ? AS ecode, ? AS counter) "
			+ "MERGE emoji_freqs_spark AS dst "
			+ "USING src ON dst.ecode = src.ecode "
			+ "WHEN MATCHED THEN UPDATE SET dst.counter += src.counter "
			+ "WHEN NOT MATCHED THEN INSERT (ecode, counter) VALUES (src.ecode, src.counter);";
    */
    
    public static void main(String[] args) {

    	if(args.length != 1) {
    		System.err.println("usage: "+EmojiTrackerDB.class.getName()
    								+" [masterConfig]");
    		System.exit(-1);
    	}
    	
    	String masterConfig = args[0];
    
        SparkConf cnf = new SparkConf()
        		.setMaster(masterConfig).setAppName(
        				EmojiTrackerDB.class.getName()+ " Streaming");

        JavaStreamingContext jsc =
        		new JavaStreamingContext(cnf,Durations.seconds(5));
        jsc.checkpoint(CHECKPOINT_DIR);

        //create the tweet stream represented by objects of type Status (twitter4j)
        //map them to a stream of status messages only before
        //extracting all emojis that are contained
        JavaDStream<String> emojisOnly = TwitterUtils.createStream(jsc)
        			.map(t -> t.getText())
        			.flatMap(s -> EmojiUtils.extractEmojisAsString(s));
        
        //count all the emojis over a fixed period of time
        //i.e. window-size which must be a multiple of the
        //streaming context's batch duration
        JavaPairDStream<String,Long> emojiSpots = emojisOnly.mapToPair(ht -> new Tuple2<>(ht, 1L));

        JavaPairDStream<String,Long> emojiFreqs = emojiSpots.reduceByKeyAndWindow(
                (c1,c2)->(c1+c2),
                Durations.seconds(5),//window size
                Durations.seconds(5) //sliding interval
        );

        //console output for demo and convenience only
        emojiFreqs.foreachRDD(pairRDD ->
				pairRDD.foreachPartition(
					p -> p.forEachRemaining(System.out::println)));
        
        //we continously update the database with the
        //resulting counts in the corresponding window 
        emojiFreqs.foreachRDD(
        		pairRDD -> {
        				pairRDD.foreachPartition(p -> {
        					try (
	        					Connection dbCon = DriverManager.getConnection(JDBC_CONNECTION);
        						PreparedStatement upsert = dbCon.prepareStatement(SQL_UPSERT_EMOJI_COUNT);
        						) {
        						dbCon.setAutoCommit(false);
        						while(p.hasNext()) {
	        						Tuple2<String,Long> t = p.next();
	        						upsert.setString(1, EmojiParser.parseToHtmlHexadecimal(t._1));
									upsert.setLong(2, t._2);
	        						upsert.setLong(3, t._2);
	        						upsert.addBatch();
	        					}
	        					int[] result = upsert.executeBatch();
	        					System.err.println("rows upserted: "+ result.length);
	        					dbCon.commit();
        					} catch(Exception e) {
        						e.printStackTrace();
        					}
        				});
        		}
        );
        		
        jsc.start();
       
        //! FOR DEMO PURPOSES ONLY !
        //stops the streaming job after certain
        //time period e.g. 10 minutes
        jsc.awaitTerminationOrTimeout(Durations.minutes(10).milliseconds());
        
        jsc.stop();

    }
    
}
