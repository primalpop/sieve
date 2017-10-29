package db;

import model.policy.BEPolicy;
import model.policy.BooleanCondition;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Created by cygnus on 9/25/17.
 *  Ref: https://codereview.stackexchange.com/questions/82713/database-abstraction-in-java
 */
public class SQLPersister implements Persister {

    private Connection connection;
    private Properties props;
    private PreparedStatement prepStatement;
    private ResultSet resultSet;
    private Statement statement;

    public SQLPersister() throws SQLException, ClassNotFoundException, IOException {
        props = new Properties();
        props.load(getClass().getResourceAsStream("mysql-dev.properties"));
        connect();
        createSchemas();

    }

    private boolean isConnectionclosed() throws SQLException {
        return connection == null || connection.isClosed();
    }


    private void connect() throws ClassNotFoundException, SQLException {
        Class.forName(props.getProperty("dbdriver"));
        connection = DriverManager.getConnection(props.getProperty("dburl"),
                props.getProperty("dbuser"), props.getProperty("dbpassword"));
    }

    private void releaseResources() throws SQLException {
        if(null!=resultSet){
            resultSet.close();
        }
        if(null!=prepStatement){
            prepStatement.close();
        }
        if(null!=statement){
            statement.close();
        }
    }

    private void createSchemas() throws SQLException {
        String [] PolicySchema = {
                "CREATE TABLE IF NOT EXISTS policy (\n" +
                        "	id           varchar(255) NOT NULL PRIMARY KEY,\n" +
                        "	description  text NOT NULL,\n" +
                        "	action       text NOT NULL,\n" +
                        "	conditions 	 text NOT NULL\n" +
                        "	objects 	 text NOT NULL\n" +
                        "	queriers 	 text NOT NULL\n" +
                        "	purposes 	 text NOT NULL\n" +
                        "	authors 	 text NOT NULL\n" +
                        "	metadata 	 text NOT NULL\n" +
                        ")",
                "CREATE TABLE IF NOT EXISTS policy_condition (\n" +
                        "	name varchar(255) NOT NULL,\n" +
                        "	type   varchar(255) NOT NULL,\n" +
                        "	key   varchar(255) NOT NULL,\n" +
                        "	value   varchar(255) NOT NULL,\n" +
                        "	relop   varchar(255) NOT NULL,\n" +
                        "	policy  varchar(255) NOT NULL,\n" +
                        "	FOREIGN KEY (policy) REFERENCES policy(id) ON DELETE CASCADE\n" +
                        ")"
        };
        statement = connection.createStatement();
        statement.executeUpdate(PolicySchema[0]);
        statement.executeUpdate(PolicySchema[1]);

    }


    @Override
    public void create(BEPolicy policy) {
        // TODO Auto-generated method stub

    }

    @Override
    public BEPolicy get(String ID) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(String ID) {
        // TODO Auto-generated method stub

    }

    @Override
    public BEPolicy[] FindPoliciesForQuerier(BooleanCondition querier) {
        return new BEPolicy[0];
    }

    @Override
    public BEPolicy[] FindPoliciesforObject(BooleanCondition object) {
        return new BEPolicy[0];
    }

}
