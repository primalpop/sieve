package edu.uci.ics.tippers.db;

import java.sql.Connection;

public class PGSQLQueryManager {

    private static final Connection connection = PGSQLConnectionManager.getInstance().getConnection();

}
