package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyEngineException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by cygnus on 10/29/17.
 */
public class MySQLConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(MySQLConnectionManager.class);
    private static MySQLConnectionManager _instance = new MySQLConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;
    private static Connection connection;

    private MySQLConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("matrix/sensoria_matrix.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DATABASE = props.getProperty("database");
            USER = props.getProperty("user");
            PASSWORD = props.getProperty("password");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static MySQLConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws PolicyEngineException {
        if (connection != null)
            return connection;
        try {
            connection = DriverManager.getConnection(
                    String.format("jdbc:mysql://%s:%s/%s?useLegacyDatetimeCode=false&serverTimezone=America/Los_Angeles&rewriteBatchedStatements=true",
                            SERVER, PORT, DATABASE), USER, PASSWORD);
            System.out.println("--- Connected to " + DATABASE + " on server " + SERVER + "---");
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Connecting to MySQL");
        }
    }
}
