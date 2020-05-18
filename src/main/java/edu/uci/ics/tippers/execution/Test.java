package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.db.PGSQLConnectionManager;
import edu.uci.ics.tippers.db.PGSQLQueryManager;
import edu.uci.ics.tippers.db.QueryResult;

public class Test {


    public static void main(String args[]) {

        PGSQLConnectionManager pgsqlConnectionManager = PGSQLConnectionManager.getInstance();

        PGSQLQueryManager pgsqlQueryManager = new PGSQLQueryManager();
        QueryResult queryResult = pgsqlQueryManager.runTimedQueryExp("Select * from PRESENCE", 1);
        System.out.println(queryResult.getTimeTaken().toMillis());
    }
}
