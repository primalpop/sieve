package edu.uci.ics.tippers.dbms;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static edu.uci.ics.tippers.common.PolicyConstants.SELECT_ALL;
import static edu.uci.ics.tippers.common.PolicyConstants.SELECT_ALL_WHERE;

public class QueryManager {

    private Connection connection;
    private final QueryExecutor queryExecutor;

    public QueryManager(){
        connection = PolicyConstants.getDBMSConnection();
        queryExecutor = new QueryExecutor(connection, PolicyConstants.MAX_DURATION.getSeconds());
    }

    public Connection getConnection(){
        return connection;
    }

    public float checkSelectivity(String queryPredicates) {
        QueryResult queryResult = runTimedQueryWithOutSorting(queryPredicates, true);
        return (float) queryResult.getResultCount() / (float) PolicyConstants.getNumberOfTuples();
    }

    /**
     * for join queries
     * @param query
     * @return
     */
    public float checkSelectivityFullQuery(String query) {
        QueryResult queryResult = runTimedQueryWithOutSorting(query);
        return (float) queryResult.getResultCount() / (float) PolicyConstants.getNumberOfTuples();
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
                gList.add(queryExecutor.runWithThread(query, queryResult).getTimeTaken().toMillis());
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
                gList.add(queryExecutor.runWithThread(SELECT_ALL_WHERE + predicates,
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
            return queryExecutor.runWithThread(SELECT_ALL + predicates, queryResult).getTimeTaken();
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
            return queryExecutor.runWithThread(completeQuery, queryResult);
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
                return queryExecutor.runWithThread(SELECT_ALL + predicates, queryResult);
            else
                return queryExecutor.runWithThread(SELECT_ALL_WHERE + predicates, queryResult);
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
            return queryExecutor.runWithThread(full_query, queryResult);
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
                query = SELECT_ALL + predicates;
            else
                query = SELECT_ALL_WHERE + predicates;
            List<Long> gList = new ArrayList<>();
            for (int i = 0; i < repetitions; i++)
                gList.add(queryExecutor.runWithThread(query, queryResult).getTimeTaken().toMillis());
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

    /**
     * Simple counting query with(out) predicates
     * @param predicates
     * @return
     * @throws PolicyEngineException
     */
    public long runCountingQuery(String predicates) throws PolicyEngineException {
        try {
            if(predicates != null)
                return queryExecutor.runWithThread(SELECT_ALL_WHERE + predicates,
                        new QueryResult()).resultCount;
            else
                return queryExecutor.runWithThread(SELECT_ALL, new QueryResult()).resultCount;
        } catch (Exception e) {
            throw new PolicyEngineException("Error Running Query");
        }
    }

}