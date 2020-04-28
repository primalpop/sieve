package edu.uci.ics.tippers.db;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MySQLQueryManager {

    private static final Connection connection = MySQLConnectionManager.getInstance().getConnection();

    private QueryManager queryManager = new QueryManager(connection, 0);

    public float checkSelectivity(String queryPredicates) {
        QueryResult queryResult = runTimedQueryWithOutSorting(queryPredicates, true);
        return (float) queryResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }

    public float checkSelectivityFullQuery(String query) {
        QueryResult queryResult = runTimedQueryWithOutSorting(query);
        return (float) queryResult.getResultCount() / (float) PolicyConstants.NUMBER_OR_TUPLES;
    }

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

    /**
     * Compute the cost by execution time of the query and writes the results to file
     * @param predicates
     * @param resultCheck
     * @return
     * @throws PolicyEngineException
     */

    public QueryResult runTimedQueryWithRepetitions(String predicates, Boolean resultCheck, int repetitions) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setResultsCheck(resultCheck);
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++)
                gList.add(queryManager.runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + predicates,
                        queryResult).getTimeTaken().toMillis());
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

    /**
     * Compute the cost by execution time of the query
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */

    public Duration runTimedQuery(String predicates) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            return queryManager.runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates, queryResult).getTimeTaken();
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

    /**
     * Compute the cost by execution time of the query which includes a subquery clause
     * @param completeQuery - including the FROM clause
     * @return
     * @throws PolicyEngineException
     */

    public QueryResult runTimedSubQuery(String completeQuery, boolean resultCheck) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setResultsCheck(resultCheck);
            return queryManager.runWithThread(completeQuery, queryResult);
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }


    /**
     * Execution time for guards which includes cost of sorting the results
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */
    public QueryResult runTimedQueryWithOutSorting(String predicates, boolean where) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            if(!where)
                return queryManager.runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates, queryResult);
            else
                return queryManager.runWithThread(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + predicates, queryResult);
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

    /**
     * @param full_query
     * @return
     * @throws PolicyEngineException
     */
    public QueryResult runTimedQueryWithOutSorting(String full_query) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            return queryManager.runWithThread(full_query, queryResult);
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }


    /**
     * Execution time for guards which doesn't cost of sorting the results
     * @throws PolicyEngineException
     */
    public QueryResult executeQuery(String predicates, boolean where, int repetitions) throws PolicyEngineException {
        try {
            QueryResult queryResult = new QueryResult();
            String query;
            if (!where)
                query = PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS + predicates;
            else
                query = PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + predicates;
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++)
                gList.add(queryManager.runWithThread(query, queryResult).getTimeTaken().toMillis());
            Duration gCost;
            if (repetitions >= 3) {
                Collections.sort(gList);
                List<Long> clippedGList = gList.subList(1, repetitions - 1);
                gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            } else {
                gCost = Duration.ofMillis(gList.stream().mapToLong(i -> i).sum() / gList.size());

            }
            queryResult.setTimeTaken(gCost);
            return queryResult;
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }

    }
}