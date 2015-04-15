import twitter4j.*;

import java.util.*;

public class TweetFetcher implements StatusListener {
	/*
	 * Class Variables
	 */
	private static DBHelper dbHelper;
	private static StatusListener tweetListener;
	private static TwitterStream tweetStream;
	private static String[] keywords = {"love", "music", "friends", "hate", "rio", "london"};
	private static Map<String, List<Status>> map;
	private static Calendar nextDeletionDate = Calendar.getInstance();
	
	// Delete tweets older than this threshold
	private static final int deletionThreshSeconds = 60 * 6;
	// Execute a deletion on this interval
	private static final int deletionInterval = 60 * 6;

	/*
	 * Put the tweet in the correct list by looking the list
	 * up in the map. 
	 */
	public static void sortStatus(Status status) {				
		String upperText = status.getText().toLowerCase();
		for (String keyword : keywords)
			if (upperText.contains(keyword))
				map.get(keyword).add(status);
	}
	
	/*
	 * Setup the tweet listener to stream new tweets with our keywords
	 */
	private static void setupListener() {
		tweetListener = new StatusListener() {
	        public void onStatus(Status status) { sortStatus(status); }
	        public void onException(Exception ex) { ex.printStackTrace(); }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			public void onScrubGeo(long arg0, long arg1) {}
			public void onStallWarning(StallWarning arg0) {}
	    };

	    tweetStream = new TwitterStreamFactory().getInstance();
	    tweetStream.addListener(tweetListener);
	    FilterQuery filter = new FilterQuery();
	    filter.track(keywords);
	    tweetStream.filter(filter);
	}
	
	/*
	 * Helper method to determine the number of tweets in the map
	 * for each keyword.
	 */
	private static void printListSizes() {
		for (String keyword : map.keySet()) {
			List<Status> list = map.get(keyword);
			System.out.printf(": %d %s tweets", list.size(), keyword);
		}
		System.out.printf("\n");
	}
	
	
	/*
	 * Use the DBHelper to write the tweets to the database
	 */
	private static void writeTweets() {
		 Map<String, List<Status>> mapRef = map;
		 resetMap();
		 dbHelper.writeTweets(mapRef);
	}
	
	/*   
	 * Create a new map with new LinkedLists to collect tweets for the keywords
	 */
	private static void resetMap() {
		map = new HashMap<String, List<Status>>();
		for (String keyword : keywords) {
			map.put(keyword, new LinkedList<Status>());
		}
	}
	
	private static void beginPeriodicWrites() {
		nextDeletionDate.add(Calendar.SECOND, -deletionInterval);
		while (true) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// Print the list sizes to see what's being inserted into the database
			printListSizes();
			// Write the tweets 
			writeTweets();
			
			// Delete tweets if it's past time
			Calendar newCal = Calendar.getInstance();
			if (nextDeletionDate.compareTo(newCal) <= 0) {
				newCal.add(Calendar.SECOND, deletionInterval);
				nextDeletionDate = newCal;
				dbHelper.deleteTweetsOlderThan(deletionThreshSeconds);
			}
		}
	}
	
	public static void main(String[] args) throws TwitterException {
		dbHelper = new DBHelper(keywords);
		
		// Setup the data structure to collect the tweets 
		resetMap();
		
		// Setup the listener to start getting tweet data asynchronously
		setupListener();
		
		// Periodically write the collected tweets to the database
		beginPeriodicWrites();
	}
	
	public void onException(Exception arg0) {}
	public void onDeletionNotice(StatusDeletionNotice arg0) {}
	public void onScrubGeo(long arg0, long arg1) {}
	public void onStallWarning(StallWarning arg0) {}
	public void onStatus(Status arg0) {}
	public void onTrackLimitationNotice(int arg0) {}
}
