package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyEngineException;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PGSQLQueryManager {

    private static final Connection connection = PGSQLConnectionManager.getInstance().getConnection();

    private QueryManager queryManager = new QueryManager(connection, 0);

    /**
     * Compute the cost by execution time of the query and writes the results to file
     * @return
     * @throws PolicyEngineException
     */

    public QueryResult runTimedQueryExp(String query, int repetitions) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setResultsCheck(false);
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++) {
                gList.add(queryManager.runWithThread(query, queryResult).getTimeTaken().toMillis());
            }
            Duration gCost;
            if(repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            }
            else{
                gCost =  Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());
            }
            queryResult.setTimeTaken(gCost);
            return queryResult;
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

}
