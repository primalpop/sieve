package edu.uci.ics.tippers.model.guard.deprecated;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.dbms.QueryManager;
import edu.uci.ics.tippers.dbms.QueryResult;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class FactorSelection {

    //Original expression
    BEExpression expression;

    // Chosen factor/guard
    //TODO: Support multiple object conditions as factor
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the factor
    FactorSelection quotient;

    // Polices from the original expression that does not contain the factor
    FactorSelection remainder;

    //Approximate Cost of evaluating the expression
    double cost;

    QueryManager queryManager = new QueryManager();

    public FactorSelection(BEExpression expression) {
        this.expression = new BEExpression(expression);
        this.multiplier = new ArrayList<ObjectCondition>();
    }

    public BEExpression getExpression() {
        return expression;
    }

    public void setExpression(BEExpression expression) {
        this.expression = expression;
    }

    public List<ObjectCondition> getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(List<ObjectCondition> multiplier) {
        this.multiplier = multiplier;
    }

    public FactorSelection getQuotient() {
        return quotient;
    }

    public void setQuotient(FactorSelection quotient) {
        this.quotient = quotient;
    }

    public FactorSelection getRemainder() {
        return remainder;
    }

    public void setRemainder(FactorSelection remainder) {
        this.remainder = remainder;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public void selectGuards(Boolean evalOnly) {
        Set<ObjectCondition> singletonSet = this.expression.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.ATTRIBUTES.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        oneTimeFactor(singletonSet, evalOnly);
    }

    private boolean isFactorGood(BEExpression original, ObjectCondition factor, boolean quotient){
        double tCost = original.estimateCostForSelection(quotient);
        double fCost = original.estimateCostOfGuardRep(factor, quotient);
        return tCost > fCost;
    }


    /**
     * Factorization based on a single object condition and not all the possible combinations
     * After selecting factor, they are not removed from the quotient
     */
    public void selectFactor(Set<ObjectCondition> objectConditionSet, boolean quotient) {
        Boolean factorized = false;
        FactorSelection currentBestFactor = new FactorSelection(this.expression);
        currentBestFactor.setCost(currentBestFactor.expression.estimateCostForSelection(false));
        Set<ObjectCondition> removal = new HashSet<>();
        for (ObjectCondition objectCondition : objectConditionSet) {
            BEExpression tempQuotient = new BEExpression(this.expression);
            tempQuotient.checkAgainstPolices(objectCondition);
            if (tempQuotient.getPolicies().size() > 1) { //was able to factorize
                if (isFactorGood(tempQuotient, objectCondition, quotient)) { //factorized is better than original
                    BEExpression tempRemainder = new BEExpression(this.expression);
                    tempRemainder.getPolicies().removeAll(tempQuotient.getPolicies());
                    double totalCost = tempQuotient.estimateCostOfGuardRep(objectCondition, quotient)
                            + tempRemainder.estimateCostForSelection(quotient);
                    if (currentBestFactor.cost > totalCost) { //greedy checking
                        factorized = true;
                        currentBestFactor.multiplier = new ArrayList<>();
                        currentBestFactor.multiplier.add(objectCondition);
                        currentBestFactor.remainder = new FactorSelection(this.expression);
                        currentBestFactor.remainder.expression.getPolicies().removeAll(tempQuotient.getPolicies());
                        currentBestFactor.quotient = new FactorSelection(tempQuotient);
                        currentBestFactor.quotient.expression.removeFromPolicies(objectCondition); //added to debug
                        currentBestFactor.cost = totalCost;
                    }
                }
//                else removal.add(objectCondition); //not considered for factorization recursively
            }
            else removal.add(objectCondition); //not a factor of at least two policies
        }
        if (factorized) {
            this.setMultiplier(currentBestFactor.getMultiplier());
            this.setQuotient(currentBestFactor.getQuotient());
            this.setRemainder(currentBestFactor.getRemainder());
            this.setCost(currentBestFactor.cost);
            removal.add(this.getMultiplier().get(0));
            objectConditionSet.removeAll(removal);
            this.remainder.selectFactor(objectConditionSet, false);
            this.quotient.selectFactor(objectConditionSet, true); //added to debug
        }
    }


    public String createQueryFromExactFactor() {
        if (multiplier.isEmpty()) {
            if (expression != null) {
                BEExpression remExp = new BEExpression(this.expression);
                remExp.removeDuplicates();
                return remExp.createQueryFromPolices();
            } else
                return "";
        }
        StringBuilder query = new StringBuilder();
        for (ObjectCondition mul : multiplier) {
            query.append(mul.print());
            query.append(PolicyConstants.CONJUNCTION);
        }
        query.append("(");
        query.append(this.quotient.createQueryFromExactFactor());
        query.append(")");
        if (!this.remainder.expression.getPolicies().isEmpty()) {
            query.append(PolicyConstants.DISJUNCTION);
            query.append("(");
            query.append(this.remainder.createQueryFromExactFactor());
            query.append(")");
        }
        return query.toString();
    }

    /**
     * returns a map with key as guards and value as the guarded representation of the partition of policies
     * guard is a single object condition
     *
     * @return
     */
    public HashMap<ObjectCondition, BEExpression> getGuardPartitionMapWithRemainder() {
        if (this.getMultiplier().isEmpty()) {
            HashMap<ObjectCondition, BEExpression> remainderMap = new HashMap<>();
            for (BEPolicy bePolicy : this.expression.getPolicies()) {
                double freq = PolicyConstants.getNumberOfTuples();
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                    if (!PolicyConstants.ATTRIBUTES.contains(oc.getAttribute())) continue;
                    if (oc.computeL() < freq) {
                        freq = oc.computeL();
                        gOC = oc;
                    }
                }
                BEExpression quo = new BEExpression();
                quo.getPolicies().add(bePolicy);
                remainderMap.put(gOC, quo);
            }
            return remainderMap;
        }
        HashMap<ObjectCondition, BEExpression> gMap = new HashMap<>();
        gMap.put(this.getMultiplier().get(0), this.getQuotient().getExpression());
        if (!this.getRemainder().expression.getPolicies().isEmpty()) {
            gMap.putAll(this.getRemainder().getGuardPartitionMapWithRemainder());
        }
        return gMap;
    }

    /**
     * If multiplier exist, it is ANDed with exact factor query of the quotient
     * If multiplier doesn't exist, select the predicate in the policy with highest selectivity as guard, remaining same as earlier
     *
     * @return a list of queries to be executed based on the guards generated
     */
    public List<String> getGuardQueries() {
        if (this.getMultiplier().isEmpty()) {
            List<String> rQueries = new ArrayList<>();
            for (BEPolicy bePolicy : this.expression.getPolicies()) {
                double freq = PolicyConstants.getNumberOfTuples();
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                    if (!PolicyConstants.ATTRIBUTES.contains(oc.getAttribute())) continue;
                    if (oc.computeL() < freq) {
                        freq = oc.computeL();
                        gOC = oc;
                    }
                }
                BEExpression quo = new BEExpression();
                quo.getPolicies().add(bePolicy);
                rQueries.add(createCleanQueryFromGQ(gOC, quo));
            }
            return rQueries;
        }
        List<String> gQueries = new ArrayList<>();
        StringBuilder gQuery = new StringBuilder();
        gQuery.append(this.getMultiplier().get(0).print());
        gQuery.append(PolicyConstants.CONJUNCTION);
        gQuery.append("(");
        if(!this.getQuotient().getMultiplier().isEmpty()) {
            System.out.println("Nested guard: " + this.getQuotient().createQueryFromExactFactor());
        }
        gQuery.append(this.getQuotient().createQueryFromExactFactor());
        gQuery.append(")");
        gQueries.add(gQuery.toString());
        if (!this.getRemainder().expression.getPolicies().isEmpty()) {
            gQueries.addAll(this.getRemainder().getGuardQueries());
        }
        return gQueries;
    }

    /**
     * Creates a query by AND'ing the guard and partition and removing all the duplicates in the partition
     *
     * @param guard
     * @param partition
     * @return
     */
    public String createCleanQueryFromGQ(ObjectCondition guard, BEExpression partition) {
        StringBuilder query = new StringBuilder();
        query.append(guard.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        partition.removeDuplicates();
        query.append(partition.createQueryFromPolices());
        query.append(")");
//        System.out.println(query.toString());
        return query.toString();
    }


    /**
     * Computes the cost of execution of individual guards and sums them up
     * For the remainder it considers the predicate with highest selectivity as the guard and computes the cost
     * Repeats the cost computation *repetitions* number of times and drops highest and lowest value to smooth it out
     *
     * @return total time taken for evaluating guards
     */
    public Duration computeGuardCosts() {
        int repetitions = 5;
        Map<ObjectCondition, BEExpression> gMap = getGuardPartitionMapWithRemainder();
        Duration rcost = Duration.ofMillis(0);
        for (ObjectCondition kOb : gMap.keySet()) {
            List<Long> cList = new ArrayList<>();
            QueryResult queryResult = new QueryResult();
            for (int i = 0; i < repetitions; i++) {
                queryResult = queryManager.runTimedQueryWithOutSorting(createCleanQueryFromGQ(kOb, gMap.get(kOb)), true);
                cList.add(queryResult.getTimeTaken().toMillis());
            }
            Collections.sort(cList);
            List<Long> clippedList = cList.subList(1, repetitions - 1);
            Duration gCost = Duration.ofMillis(clippedList.stream().mapToLong(i -> i).sum() / clippedList.size());
            rcost = rcost.plus(gCost);
        }
        return rcost;
    }

    /**
     * Works only with non-nested guards
     * Prints the following to a csv file
     * - For each guard
     * - Number of policies in the partition
     * - Number of predicates in policies
     * - Results returned by the guard
     * - Results returned by the guard + partition
     * - Time taken by each guard
     * - Time taken by each guard + partition
     * - Print the guard + partition
     * @return an arraylist of strings with each element representing a line in the csv file
     */
    public List<String> printDetailedGuardResults() {
        List<String> guardResults = new ArrayList<>();
        Map<ObjectCondition, BEExpression> gMap = getGuardPartitionMapWithRemainder();
        int repetitions = 5;
        Duration totalEval = Duration.ofMillis(0);
        for (ObjectCondition kOb : gMap.keySet()) {
            System.out.println("Executing Guard " + kOb.print());
            StringBuilder guardString = new StringBuilder();
            guardString.append(gMap.get(kOb).getPolicies().size());
            guardString.append(",");
            int numOfPreds = gMap.get(kOb).getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum();
            guardString.append(numOfPreds);
            guardString.append(",");
            List<Long> gList = new ArrayList<>();
            List<Long> cList = new ArrayList<>();
            int gCount = 0, tCount = 0;
            for (int i = 0; i < repetitions; i++) {
                QueryResult guardResult = queryManager.runTimedQueryWithOutSorting(kOb.print(), true);
                if (gCount == 0) gCount = guardResult.getResultCount();
                gList.add(guardResult.getTimeTaken().toMillis());
                QueryResult completeResult = queryManager.runTimedQueryWithOutSorting(createCleanQueryFromGQ(kOb, gMap.get(kOb)), true);
                if (tCount == 0) tCount = completeResult.getResultCount();
                cList.add(completeResult.getTimeTaken().toMillis());

            }
            Collections.sort(gList);
            List<Long> clippedGList = gList.subList(1, repetitions - 1);
            Duration gCost = Duration.ofMillis(clippedGList.stream().mapToLong(i -> i).sum() / clippedGList.size());
            Collections.sort(cList);
            List<Long> clippedCList = cList.subList(1, repetitions - 1);
            Duration gAndPcost = Duration.ofMillis(clippedCList.stream().mapToLong(i -> i).sum() / clippedCList.size());

            guardString.append(gCount);
            guardString.append(",");
            guardString.append(tCount);
            guardString.append(",");

            guardString.append(gCost.toMillis());
            guardString.append(",");
            guardString.append(gAndPcost.toMillis());
            guardString.append(",");
            guardString.append(createCleanQueryFromGQ(kOb, gMap.get(kOb)));
            guardResults.add(guardString.toString());
            totalEval = totalEval.plus(gAndPcost);
        }
        System.out.println("Total Guard Evaluation time: " + totalEval);
        guardResults.add("Total Guard Evaluation time," + totalEval.toMillis());
        return guardResults;
    }

    public List<ObjectCondition> getIndexFilters() {
        if (multiplier.isEmpty()) {
            return multiplier;
        }
        List<ObjectCondition> indexFilters = new ArrayList<>();
        for (ObjectCondition mul : multiplier) {
            indexFilters.add(mul);
        }
        if (!this.remainder.expression.getPolicies().isEmpty()) {
            indexFilters.addAll(this.remainder.getIndexFilters());
        }
        return indexFilters;
    }


    /**
     * Factorization based on a single object condition and not all the possible combinations
     * Used for nested factorization and factorizes only once
     */
    public void oneTimeFactor(Set<ObjectCondition> objectConditionSet, boolean quotient) {
        boolean factorized = false;
        FactorSelection currentBestFactor = new FactorSelection(this.expression);
        currentBestFactor.setCost(currentBestFactor.expression.estimateCostForSelection(false));
        for (ObjectCondition objectCondition : objectConditionSet) {
            BEExpression tempQuotient = new BEExpression(this.expression);
            tempQuotient.checkAgainstPolices(objectCondition);
            if (tempQuotient.getPolicies().size() > 1) { //was able to factorize
                if (isFactorGood(tempQuotient, objectCondition, quotient)) { //factorized is better than original
                    BEExpression tempRemainder = new BEExpression(this.expression);
                    tempRemainder.getPolicies().removeAll(tempQuotient.getPolicies());
                    double totalCost = tempQuotient.estimateCostOfGuardRep(objectCondition, quotient)
                            + tempRemainder.estimateCostForSelection(quotient);
                    if (currentBestFactor.cost > totalCost) { //greedy checking
                        factorized = true;
                        currentBestFactor.multiplier = new ArrayList<>();
                        currentBestFactor.multiplier.add(objectCondition);
                        currentBestFactor.remainder = new FactorSelection(this.expression);
                        currentBestFactor.remainder.expression.getPolicies().removeAll(tempQuotient.getPolicies());
                        currentBestFactor.quotient = new FactorSelection(tempQuotient);
                        currentBestFactor.quotient.expression.removeFromPolicies(objectCondition); //added to debug
                        currentBestFactor.cost = totalCost;
                    }
                }
            }
        }
        if (factorized) {
            this.setMultiplier(currentBestFactor.getMultiplier());
            this.setQuotient(currentBestFactor.getQuotient());
            this.setRemainder(currentBestFactor.getRemainder());
            this.setCost(currentBestFactor.cost);
        }
    }

}