package edu.uci.ics.tippers.dbms.db2;

import edu.uci.ics.tippers.common.PolicyEngineException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by cygnus on 12/15/17.
 */
public class DB2ConnectionManager {

    private static DB2ConnectionManager _instance = new DB2ConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;
    private static Connection connection;

    private DB2ConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("db2/db2.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DATABASE = props.getProperty("database");
            USER = props.getProperty("user");
            PASSWORD = props.getProperty("password");

        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    public static DB2ConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws PolicyEngineException {
        if (connection != null)
            return connection;
        try {
            connection = DriverManager.getConnection(
                    String.format("jdbc:db2://%s:%s/%s", SERVER, PORT, DATABASE), USER, PASSWORD);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PolicyEngineException("Error Connecting to DB2");
        }
    }

}
