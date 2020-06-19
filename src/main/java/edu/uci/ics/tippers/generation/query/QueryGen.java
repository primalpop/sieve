package edu.uci.ics.tippers.generation.query;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class QueryGen {


    public QueryManager queryManager;
    public Connection connection;
    Random r;
    double lowSelDown, lowSelUp;
    double medSelDown, medSelUp;
    double highSelDown, highSelUp;


    public QueryGen() {
        PolicyConstants.initialize();
        connection = PolicyConstants.getDBMSConnection();

        lowSelDown = 0.00001;
        lowSelUp = 0.001;
        medSelDown = 0.001;
        medSelUp = 0.2;
        highSelDown = 0.3;
        highSelUp = 0.5;

        this.queryManager = new QueryManager();
        r = new Random();
    }

    public double getSelectivity(String selType){
        if(selType.equalsIgnoreCase("low")){
            return lowSelDown + Math.random() * (lowSelUp - lowSelDown);
        }
        else if(selType.equalsIgnoreCase("medium")){
            return medSelDown + Math.random() * (medSelUp - medSelDown);
        }
        else
            return highSelDown + Math.random() * (highSelUp - highSelDown);
    }

    public String checkStaticRangeSelectivity(double selectivity) {
        String selType = null;
        if (lowSelDown < selectivity && selectivity < lowSelUp) selType = "low";
        if (medSelDown < selectivity && selectivity < medSelUp) selType = "medium";
        if (highSelDown < selectivity && selectivity < highSelUp) selType = "high";
        return selType;
    }

    public String checkSelectivityType(double chosenSel, double selectivity) {
        String selType = null;
        if (chosenSel/10 < selectivity && selectivity < chosenSel) selType = "low";
        if (chosenSel/5 < selectivity && selectivity < chosenSel * 5) selType = "medium";
        if (chosenSel/2 < selectivity) selType = "high";
        return selType;
    }


    public String checkSelectivityType(String chosenSelType, double chosenSel, double selectivity) {
        String selType = null;
        if (chosenSel/10 < selectivity && selectivity < chosenSel) selType = "low";
        if (chosenSel/5 < selectivity && selectivity < chosenSel * 5) selType = "medium";
        if (chosenSel/2 < selectivity) selType = "high";
        return selType;
    }

    public List<QueryStatement> getQueries(int templateNum, List<String> selTypes, int numOfQueries) {
        if (templateNum == 0) {
            return createQuery1(selTypes, numOfQueries);
        } else if (templateNum == 1) {
            return createQuery2(selTypes, numOfQueries);
        } else if (templateNum == 2) {
            return createQuery3(selTypes, numOfQueries);
        } else if (templateNum == 3) {
            return createQuery4();
        }
        return null;
    }

    public abstract List<QueryStatement> createQuery1(List<String> selTypes, int numOfQueries);

    public abstract List<QueryStatement> createQuery2(List<String> selTypes, int numOfQueries);

    public abstract List<QueryStatement> createQuery3(List<String> selTypes, int numOfQueries);

    public abstract List<QueryStatement> createQuery4();

    public void insertQuery(List<QueryStatement> queryStatements) {
        String soInsert = "INSERT INTO QUERIES " +
                "(query_statement, template, selectivity, selectivity_type, inserted_at) " +
                "VALUES (?, ?, ?, ?, ?)";
        try {

            PreparedStatement stm = connection.prepareStatement(soInsert);
            int queryCount = 0;
            for (QueryStatement qs : queryStatements) {
                stm.setString(1, qs.getQuery());
                stm.setInt(2, qs.getTemplate());
                stm.setFloat(3, qs.getSelectivity());
                stm.setString(4, qs.getSelectivity_type());
                stm.setTimestamp(5, qs.getInserted_at());
                stm.addBatch();
                queryCount++;
                if (queryCount % 100 == 0) {
                    stm.executeBatch();
                    System.out.println("# " + queryCount + " inserted");
                }
            }
            stm.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public List<QueryStatement> retrieveQueries(int template, String selectivity_type, int query_count) {
        List<QueryStatement> queryStatements = new ArrayList<>();
        PreparedStatement queryStm = null;
        try {
            if (selectivity_type.equalsIgnoreCase("all")) {
                queryStm = connection.prepareStatement("SELECT id, query_statement, selectivity FROM QUERIES as q " +
                        "WHERE q.template = ? order by selectivity limit " + query_count);
                queryStm.setInt(1, template);
            }
            else {
                queryStm = connection.prepareStatement("SELECT id, query_statement, selectivity FROM QUERIES as q " +
                        "WHERE q.selectivity_type = ? AND q.template = ? order by selectivity limit " + query_count);
                queryStm.setString(1, selectivity_type);
                queryStm.setInt(2, template);
            }
            ResultSet rs = queryStm.executeQuery();
            while (rs.next()) {
                QueryStatement qs = new QueryStatement();
                qs.setQuery(rs.getString("query_statement"));
                qs.setId(rs.getInt("id"));
                qs.setSelectivity(rs.getFloat("selectivity"));
                qs.setTemplate(template);
                queryStatements.add(qs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return queryStatements;
    }


    public void constructWorkload(boolean[] templates, int numOfQueries) {
        List<String> selTypes = new ArrayList<>();
        selTypes.add("low");
        selTypes.add("medium");
        selTypes.add("high");
        List<QueryStatement> queries = new ArrayList<>();
        for (int i = 0; i < templates.length; i++) {
            if (templates[i]) queries.addAll(getQueries(i, selTypes, numOfQueries));
        }
        insertQuery(queries);
    }
}
