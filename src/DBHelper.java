import java.sql.*;

import twitter4j.*;

import java.util.*;


public class DBHelper {	
	// Constants use to add noise to the location data
	private final double rad_Earth  = 6378.16;
	private final double one_degree = (2 * Math.PI * rad_Earth) / 360;
	private final double one_km     = 1 / one_degree;
	
	private Map<String, Integer> keywordIDs = new HashMap<String, Integer>();
	private HashMap<String, double[]> keywordLoc = new HashMap<String, double[]>();
	private java.sql.Connection conn;
	private QueueMessenger queueMessenger = new QueueMessenger();
		
	public DBHelper(String[] keywords) {
		// Make sure the keywords are already in the database
		writeKeywords(keywords);
		// Set initial locations of tweets for each keyword to NYC
		setupInitialLocations(keywords);
		// Get the keyword IDs so you don't have to do this repeatedly before inserts
		getKeywordIDs();
	}
	
	/*
	 * Sets the initial location for our keywords to NYC
	 */
	private void setupInitialLocations(String[] keywords) {
		for (String keyword : keywords) {
			double[] location = {40.7127, -74.0059};
			keywordLoc.put(keyword, location);
		}
	}
	
	/*
	 * Get the IDs for the keys at the start to save on queries later.
	 */
	private void getKeywordIDs() {
		setupConnection();
		try {
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT * FROM Keywords;");
			while (results.next()) {
				String key = results.getString("keyword");
				int id     = results.getInt("key_id");
				keywordIDs.put(key, id);
			}
			stmt.close();
			
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
		}
	}
	
	public void deleteTweetsOlderThan(int secs) {
		setupConnection();
		try {
			Statement stmt = conn.createStatement();
			String delete = "DELETE FROM Tweets " +
			                "WHERE date_time < from_unixtime(unix_timestamp() - " + secs + ");";
			stmt.executeUpdate(delete);
			stmt.close();
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
			if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
		}
	}

	public List<String> getExistingKeywords() {
		List<String> keywords = new LinkedList<String>();
		setupConnection();
		
		try {
			Statement stmt = conn.createStatement();
			ResultSet results = stmt.executeQuery("SELECT keyword FROM Keywords;");
			System.out.println("Getting existing keywords");

			while (results.next()) 
				keywords.add(results.getString("keyword"));
			stmt.close();
			
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
		}
		
		return keywords;
	}
	
	public void writeKeywords(String[] keywords) {
		if (keywords.length == 0) return;
		
		List<String> existingKeywords = getExistingKeywords();
		setupConnection();
		
		try {
			Statement stmt = conn.createStatement();
			
			for (String keyword : keywords) {
				if (!existingKeywords.contains(keyword)) {
					String insert = "INSERT INTO Keywords VALUES (NULL, '" + keyword + "');";
					stmt.addBatch(insert);
				}
			}
			stmt.executeBatch();
			stmt.close();
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
		}
	}
	
	private double getRandomNoise() {
		double noise = one_km * 15 * Math.random();
		if (Math.random() < .5) 
			noise *= -1;
		return noise;
	}
	
	private double[] addNoiseToLocation(double[] loc) {
		loc[0] += getRandomNoise();
		loc[1] += getRandomNoise();
		return loc;
	}
	
	/*
	 * If a status has a location, set the last location for this keyword to its
	 * latitude and longitude. This will apply to future tweets with this keyword
	 * that do not have a geolocation.
	 */
	private void updateLocationForKeyword(String keyword, Status status) {
		GeoLocation geo = status.getGeoLocation();
		if (geo != null) { 
			// Update the location for this keyword
			double[] newLoc = {geo.getLatitude(), geo.getLongitude()};
			keywordLoc.put(keyword, newLoc);
		} else {
			double[] noisyLocation = addNoiseToLocation(keywordLoc.get(keyword));
			keywordLoc.put(keyword, noisyLocation);
		}
	}
	
	// Get the number of tweets that we'll be inserting 
	private int getTweetCount(Map<String, List<Status>> map) {
		int count = 0;
		for (String key : map.keySet())
			count += map.get(key).size();
		return count;
	}
	
	// Create string for prepared statement with tweet value placeholders
	private String getInsertSqlStringForTweets(Map<String, List<Status>> map) {
		int count = getTweetCount(map);
		StringBuffer sql = new StringBuffer("INSERT INTO Tweets VALUES (NULL, ?, ?, DEFAULT, ?, ?)");
		for (int i = 1; i < count; i++)
			sql.append(", (NULL, ?, ?, DEFAULT, ?, ?)");
		return sql.toString();
	}
	
	// Format the prepared statement with tweet values
	private void configurePreparedStatement(PreparedStatement pstmt, 
			Map<String, List<Status>> map) throws SQLException {
		int i = 1;
		
		for (String keyword : map.keySet()) {
			List<Status> list = map.get(keyword);
			int keyId = keywordIDs.get(keyword);
			
			for (Status status : list) {
				updateLocationForKeyword(keyword, status);
				double[] loc = keywordLoc.get(keyword);
				pstmt.setLong(i++, status.getId());
				pstmt.setInt(i++, keyId);
				pstmt.setDouble(i++, loc[0]);
				pstmt.setDouble(i++, loc[1]);
			}
		}
	}
	
	public void writeTweets(Map<String, List<Status>> map) {
		if (map.isEmpty()) return;
		
		setupConnection();
		try {
			String sql = getInsertSqlStringForTweets(map);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			configurePreparedStatement(pstmt, map);
			pstmt.execute();
			pstmt.close();
			
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
		}
		
		
	}
	
	private void setupConnection() {	  
		String dbName   = DBInfo.dbName;
		String userName = DBInfo.userName;
		String password = DBInfo.password;
		String hostname = DBInfo.hostname;
		String port     = DBInfo.port;
		
		String jdbcUrl = "jdbc:mysql://" + hostname + ":" +
				port + "/" + dbName + "?user=" + userName + "&password=" + password;
		try {
			conn = DriverManager.getConnection(jdbcUrl);
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} 
	}
}
