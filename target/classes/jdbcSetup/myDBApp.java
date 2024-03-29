import java.io.*;
import java.sql.*;
import java.util.*;

public class myDBApp {
	
	// NOTE: You will need to change some variables from START to END.
	public static void main(String[] argv) throws SQLException {
		// START
		// Enter your username.
		String user = "<<< CHANGE ME - Username >>>";
		// Enter your database password, NOT your university password.
		String password = "<<< CHANGE ME - Password >>>";
		
		/** IMPORTANT: If you are using NoMachine, you can leave this as it is.
		 * 
	 	 *  Otherwise, if you are using your OWN COMPUTER with TUNNELLING:
	 	 * 		1) Delete the original database string and 
	 	 * 		2) Remove the '//' in front of the second database string.
	 	 */
		String database = "teachdb.cs.rhul.ac.uk";
		//String database = "localhost";
		// END
		
		Connection connection = connectToDatabase(user, password, database);
		if (connection != null) {
			System.out.println("SUCCESS: You made it!"
					+ "\n\t You can now take control of your database!\n");
		} else {
			System.out.println("ERROR: \tFailed to make connection!");
			System.exit(1);
		}
		// Now we're ready to use the DB. You may add your code below this line.
		
	}
	
	// You can write your new methods here.
	
	
	// ADVANCED: This method is for advanced users only. You should not need to change this!
	public static Connection connectToDatabase(String user, String password, String database) {
		System.out.println("------ Testing PostgreSQL JDBC Connection ------");
		Connection connection = null;
		try {
			String protocol = "jdbc:postgresql://";
			String dbName = "/CS2855/";
			String fullURL = protocol + database + dbName + user;
			connection = DriverManager.getConnection(fullURL, user, password);
		} catch (SQLException e) {
			String errorMsg = e.getMessage();
			if (errorMsg.contains("authentication failed")) {
				System.out.println("ERROR: \tDatabase password is incorrect. Have you changed the password string above?");
				System.out.println("\n\tMake sure you are NOT using your university password.\n"
						+ "\tYou need to use the password that was emailed to you!");
			} else {
				System.out.println("Connection failed! Check output console.");
				e.printStackTrace();
			}
		}
		return connection;
	}
}