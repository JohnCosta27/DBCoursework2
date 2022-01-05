import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.*;
import java.time.LocalTime;
import java.util.Scanner;

/**
 * Database Mini-project submission file.
 *
 * @author johnc
 */
public class Main {

	/**
	 *	Initliases the connection with the database.
	 *
	 * @param argv
	 * @throws SQLException
	 */
	public static void main(String[] argv) throws SQLException {

		Scanner in = new Scanner(System.in);

		System.out.print("Please enter your username: ");
		String user = in.nextLine();

		System.out.print("Please enter your password: ");
		String password = in.nextLine();

		String database = "localhost";

		Connection connection = connectToDatabase(user, password, database);
		if (connection != null) {
			System.out.println("STATUS: Database Connected!");

			initDabaseTables(connection);

		} else {
			System.out.println("ERROR: \tFailed to make connection!");
			System.exit(1);
		}

	}

	/**
	 * Executes the needed drop table commands and creates tables.
	 *
	 * @param connection is the PostgreSQL db connection object.
	 */
	public static void initDabaseTables(Connection connection) {
		try {

			Statement dropAirports = connection.createStatement();
			dropAirports.executeUpdate("DROP TABLE IF EXISTS airports, delayedFlights");
			System.out.println("UPDATE: Airports and DelayedFlights tables have been dropped");

			String createAirportTable = "CREATE TABLE airports (" +
					"airportCode VARCHAR(3) PRIMARY KEY," +
					"airportName VARCHAR(64) NOT NULL UNIQUE," +
					"city VARCHAR(64) NOT NULL," +
					"state VARCHAR(64) NOT NULL);";

			Statement createAirports = connection.createStatement();
			createAirports.executeUpdate(createAirportTable);
			System.out.println("UPDATE: Airports table has been created");

			String createDelayedFlightsTable = "CREATE TABLE delayedFlights (" +
					"id INTEGER PRIMARY KEY," +
					"month INTEGER NOT NULL," +
					"dayOfMonth INTEGER NOT NULL," +
					"dayOfWeek INTEGER NOT NULL," +
					"depTime TIME NOT NULL," +
					"scheduledDepTime TIME NOT NULL," +
					"arrTime TIME NOT NULL," +
					"scheduledArrTime TIME NOT NULL," +
					"uniqueCarrier VARCHAR(2) NOT NULL," +
					"flightNum INTEGER NOT NULL," +
					"actualFlightTime TIME NOT NULL," +
					"scheduledFlightTime TIME NOT NULL," +
					"airTime TIME NOT NULL," +
					"arrDelay INTEGER NOT NULL," +
					"depDelay INTEGER NOT NULL," +
					"orig VARCHAR(3) REFERENCES airports (airportCode)," +
					"dest VARCHAR(3) REFERENCES airports (airportCode)," +
					"distance INTEGER NOT NULL);";

			Statement createDelayedFlights = connection.createStatement();
			createDelayedFlights.executeUpdate(createDelayedFlightsTable);
			System.out.println("UPDATE: Delayed Flights table has been created");

			URL airportFileURI = Main.class.getClassLoader().getResource("airport.txt");
			URL delayedFlightsFileURI = Main.class.getClassLoader().getResource("delayedFlights");

			if (airportFileURI == null || delayedFlightsFileURI == null) {
				throw new FileNotFoundException();
			} else {

				File airportFile = new File(airportFileURI.toURI());
				BufferedReader airportFileReader = new BufferedReader(new FileReader(airportFile));
				String insertAirportQuery = "INSERT INTO airports (airportCode, airportName, city, state) " +
						"VALUES (?, ?, ?, ?);";

				String line;
				while ((line = airportFileReader.readLine()) != null) {

					String[] splitRow = line.split(",");
					PreparedStatement insertAirport = connection.prepareStatement(insertAirportQuery);

					insertAirport.setString(1, splitRow[0]);
					insertAirport.setString(2, splitRow[1]);
					insertAirport.setString(3, splitRow[2]);
					insertAirport.setString(4, splitRow[3]);

				}

				File delayedFlightsFile = new File(delayedFlightsFileURI.toURI());
				BufferedReader delayedFlightsReader = new BufferedReader(new FileReader(delayedFlightsFile));
				//18
				String insertDelayedFlightQuery = "INSERT INTO delayedFlights VALUES " +
						"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

				while ((line = delayedFlightsReader.readLine()) != null) {

					String[] splitRow = line.split(",");
					PreparedStatement insertDelayedFlight = connection.prepareStatement(insertDelayedFlightQuery);

					insertDelayedFlight.setInt(1, Integer.parseInt(splitRow[0]));
					insertDelayedFlight.setInt(2, Integer.parseInt(splitRow[1]));
					insertDelayedFlight.setInt(3, Integer.parseInt(splitRow[2]));
					insertDelayedFlight.setInt(4, Integer.parseInt(splitRow[3]));
					insertDelayedFlight.setTime(5, parseTime(splitRow[4]));
					insertDelayedFlight.setTime(6, parseTime(splitRow[5]));
					insertDelayedFlight.setTime(7, parseTime(splitRow[6]));
					insertDelayedFlight.setTime(8, parseTime(splitRow[7]));
					insertDelayedFlight.setString(9, splitRow[8]);
					insertDelayedFlight.setInt(10,  Integer.parseInt(splitRow[9]));
					insertDelayedFlight.setInt(11, Integer.parseInt(splitRow[10]));
					insertDelayedFlight.setInt(12, Integer.parseInt(splitRow[11]));
					insertDelayedFlight.setInt(13, Integer.parseInt(splitRow[12]));
					insertDelayedFlight.setInt(14, Integer.parseInt(splitRow[13]));
					insertDelayedFlight.setInt(15, Integer.parseInt(splitRow[14]));
					insertDelayedFlight.setString(16, splitRow[15]);
					insertDelayedFlight.setString(17, splitRow[16]);
					insertDelayedFlight.setInt(18, Integer.parseInt(splitRow[17]));

				}

			}

		} catch (SQLException | URISyntaxException | IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Executes the queries needed
	 *
	 * @param connection postgresql db connection object.
	 */
	public static void queries(Connection connection) {

	}

	/**
	 * Util function to parse time for delayedFlights inputs.
	 *
	 * @param stringTime in format HHMM
	 * @return Time object
	 */
	public static Time parseTime(String stringTime) {
		System.out.println(stringTime.substring(0, 2) + ":" + stringTime.substring(2) + ":00");
		return Time.valueOf(stringTime.substring(0, 2) + ":" + stringTime.substring(2) + ":00");
	}

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
