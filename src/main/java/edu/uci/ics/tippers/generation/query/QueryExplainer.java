package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.mysql.MySQLConnectionManager;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class QueryExplainer {

    private static Connection connection = MySQLConnectionManager.getInstance().getConnection();

    public QExplain access_method(QueryStatement queryStatement){
        PreparedStatement explainStm = null;
        QExplain qe = new QExplain();
        String queryPredicates = queryStatement.getQuery();
        if(queryStatement.getTemplate() == 3) {
            queryPredicates = queryPredicates.replace("polEval", "PRESENCE");
        }
        else
            queryPredicates = "SELECT * from PRESENCE where " + queryPredicates;
        try{
            explainStm = connection.prepareStatement("explain " + queryPredicates);
            ResultSet rs = explainStm.executeQuery();
            rs.next();
            if(queryStatement.getTemplate() == 3) {
                //For join query there are two lines of estimated number of rows accessed
                qe.getNum_rows().add(rs.getInt("rows"));
                rs.next();
                qe.getNum_rows().add(rs.getInt("rows"));
                qe.setAccess_method(rs.getString("key"));
            }
            else { //for template 1 or 2
                qe.getNum_rows().add(rs.getInt("rows"));
                qe.setAccess_method(rs.getString("key"));
            }
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

    /**
     * Very poor estimate of selectivity based on explain of a query
     * if key == null: return 0.3 //Linear Scan
     * else return (multiply all rows in num_rows) / DB
     * If template 1 or 2, then multiplication makes no difference
     * If template 3, then computes the total number of rows in the cross product
     * @param queryStatement
     * @return
     */
    public double estimateSelectivity(QueryStatement queryStatement){
        QExplain qe = access_method(queryStatement);
        if(qe.getAccess_method() == null) return 0.3; //Linear scan
        else {
            int total_rows = qe.getNum_rows().stream().reduce(1, (a, b) -> a * b);
            return (double) (total_rows)/(PolicyConstants.getNumberOfTuples()); //Index scan
        }
    }

    public String keyUsed(QueryStatement queryStatement){
        QExplain qe = access_method(queryStatement);
        if(qe.getAccess_method()!= null && qe.getAccess_method().contains(",")){
            return qe.getAccess_method().split(",")[0];
        }
        return qe.getAccess_method();
    }


    class QExplain {
        String access_method;
        List<Integer> num_rows;

        public QExplain(String access_method, List<Integer> num_rows, double filtered) {
            this.access_method = access_method;
            this.num_rows = num_rows;
        }

        public QExplain() {
            this.access_method = "";
            this.num_rows = new ArrayList<>();
        }

        public String getAccess_method() {
            return access_method;
        }

        public void setAccess_method(String access_method) {
            this.access_method = access_method;
        }

        public List<Integer> getNum_rows() {
            return num_rows;
        }

        public void setNum_rows(List<Integer> num_rows) {
            this.num_rows = num_rows;
        }
    }

}
