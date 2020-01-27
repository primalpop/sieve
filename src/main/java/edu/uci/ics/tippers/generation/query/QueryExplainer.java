package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;

import java.sql.*;

public class QueryExplainer {

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();
    private static final int NUMBER_OF_BLOCKS = 6365;

    public QExplain access_method(String query){
        PreparedStatement explainStm = null;
        QExplain qe = new QExplain();
        try{
            explainStm = connection.prepareStatement("explain " + query);
            ResultSet rs = explainStm.executeQuery();
            rs.next();
            qe.setAccess_method(rs.getString("key"));
            qe.setNum_rows(rs.getInt("rows"));

        } catch(SQLException e) {
            e.printStackTrace();
        }
        return qe;
    }

    public String printExplain(String query){
        PreparedStatement explainStm = null;
        StringBuilder exResult = new StringBuilder();
        try {
            explainStm = connection.prepareStatement("explain " + query);
            ResultSet rs = explainStm.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    Object object = rs.getObject(columnIndex);
                    exResult.append(String.format("%s, ", object == null ? "NULL" : object.toString()));
                }
                exResult.append("\n");
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return exResult.toString();
    }

    public double estimateCost(String query){
        double costQuery = 0.0;
        QExplain qe = access_method(query);
        if(qe.getAccess_method() == null) costQuery = NUMBER_OF_BLOCKS * PolicyConstants.IO_BLOCK_READ_COST;
        else costQuery = qe.num_rows * PolicyConstants.IO_BLOCK_READ_COST;
        return costQuery;
    }

    public String keyUsed(String query){
        QExplain qe = access_method(query);
        return qe.getAccess_method();
    }


    class QExplain {
        String access_method;
        int num_rows;

        public QExplain(String access_method, int num_rows) {
            this.access_method = access_method;
            this.num_rows = num_rows;
        }

        public QExplain() {

        }

        public String getAccess_method() {
            return access_method;
        }

        public void setAccess_method(String access_method) {
            this.access_method = access_method;
        }

        public int getNum_rows() {
            return num_rows;
        }

        public void setNum_rows(int num_rows) {
            this.num_rows = num_rows;
        }
    }

}
