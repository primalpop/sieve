package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryExplainer {

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();
    private static final int NUMBER_OF_BLOCKS = 6365;

    private QExplain access_method(String query){
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

    //TODO: Double check cost of table scan and index scan formulas
    public double estimateCost(String query){
        double costQuery = 0.0;
        QExplain qe = access_method(query);
        if(qe.getAccess_method() == null) costQuery = NUMBER_OF_BLOCKS * PolicyConstants.IO_BLOCK_READ_COST;
        else costQuery = qe.num_rows * PolicyConstants.IO_BLOCK_READ_COST;
        return costQuery;
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
