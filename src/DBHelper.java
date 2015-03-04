import java.sql.*;
import twitter4j.*;
import java.util.*;


public class DBHelper {
	// Holds the IDs for each keyword in the database
	private Map<String, Integer> keywordIDs = new HashMap<String, Integer>();
	// Holds the current (lat, lng) for each keyword
	private HashMap<String, double[]> keywordLoc = new HashMap<String, double[]>();
	private java.sql.Connection conn;
	private Statement setupStatement;
	private Statement readStatement;
	private ResultSet resultSet;
	
	public DBHelper(String[] keywords) {
		writeKeywords(keywords);
		setupInitialLocations(keywords);
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
			readStatement = conn.createStatement();
			resultSet = readStatement.executeQuery("SELECT * FROM Keywords;");
			while (resultSet.next()) {
				String key = resultSet.getString("keyword");
				int id     = resultSet.getInt("key_id");
				keywordIDs.put(key, id);
			}
		    readStatement.close();
			
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
			setupStatement = conn.createStatement();
			String delete = "DELETE FROM Tweets " +
			                "WHERE date_time < from_unixtime(unix_timestamp() - " + secs + ");";
			setupStatement.executeUpdate(delete);
			setupStatement.close();
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
			readStatement = conn.createStatement();
			resultSet = readStatement.executeQuery("SELECT keyword FROM Keywords;");
			System.out.println("Getting existing keywords");

			while (resultSet.next()) 
				keywords.add(resultSet.getString("keyword"));
		    readStatement.close();
			
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
			setupStatement = conn.createStatement();
			
			for (String keyword : keywords) {
				if (!existingKeywords.contains(keyword)) {
					String insert = "INSERT INTO Keywords VALUES (NULL, '" + keyword + "');";
					setupStatement.addBatch(insert);
				}
			}
			setupStatement.executeBatch();
			setupStatement.close();
		} catch (SQLException ex) {
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		} finally {
		    if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
		}
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
		} 
	}
	
	public void writeTweets(Map<String, List<Status>> map) {
		if (map.isEmpty()) return;
		System.out.println("Preparing to write tweets.");
		StringBuffer sql = new StringBuffer("INSERT INTO Tweets VALUES (NULL, ?, DEFAULT, ?, ?)");
		int count = 0;
		for (String key : map.keySet())
			count += map.get(key).size();
		for (int i = 1; i < count; i++)
			sql.append(", (NULL, ?, DEFAULT, ?, ?)");
		
		setupConnection();
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql.toString());
			int keyId;
			int i = 1;
			for (String keyword : map.keySet()) {
				List<Status> list = map.get(keyword);
				keyId = keywordIDs.get(keyword);
				
				for (Status status : list) {
					updateLocationForKeyword(keyword, status);
					double[] loc = keywordLoc.get(keyword);
					pstmt.setInt(i++, keyId);
					pstmt.setDouble(i++, loc[0]);
					pstmt.setDouble(i++, loc[1]);
				}
			}
			pstmt.execute();
			System.out.println("Wrote new tweets.");
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
		/*
	  	  Perhaps use System.getProperties("key") here
		*/
	  
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
