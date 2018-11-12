package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.model.data.Presence;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.BooleanPredicate;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
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

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

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

    public void selectGuards() {
        Set<ObjectCondition> singletonSet = this.expression.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.INDEXED_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        selectFactor(singletonSet);
    }

    /**
     * Factorization based on a single object condition and not all the possible combinations
     * After selecting factor, they are not removed from the quotient
     */
    public void selectFactor(Set<ObjectCondition> objectConditionSet) {
        Boolean factorized = false;
        FactorSelection currentBestFactor = new FactorSelection(this.expression);
        currentBestFactor.setCost(Double.POSITIVE_INFINITY);
        Set<ObjectCondition> removal = new HashSet<>();
        for (ObjectCondition objectCondition : objectConditionSet) {
            BEExpression temp = new BEExpression(this.expression);
            temp.checkAgainstPolices(objectCondition);
            if (temp.getPolicies().size() > 1) { //was able to factorize
                double tCost = temp.estimateCostForSelection(objectCondition);
                double fCost = temp.estimateCostOfGuardRep(objectCondition);
                if (tCost > fCost) {
                    if (currentBestFactor.cost > fCost) {
                        factorized = true;
                        currentBestFactor.multiplier = new ArrayList<>();
                        currentBestFactor.multiplier.add(objectCondition);
                        currentBestFactor.remainder = new FactorSelection(this.expression);
                        currentBestFactor.remainder.expression.getPolicies().removeAll(temp.getPolicies());
                        currentBestFactor.quotient = new FactorSelection(temp);
//                        currentBestFactor.quotient.expression.removeFromPolicies(objectCondition);
                        currentBestFactor.cost = fCost;
                    }
                } else removal.add(objectCondition); //not considered for factorization recursively
            } else removal.add(objectCondition); //not a factor of at least two policies
        }
        if (factorized) {
            this.setMultiplier(currentBestFactor.getMultiplier());
            this.setQuotient(currentBestFactor.getQuotient());
            this.setRemainder(currentBestFactor.getRemainder());
            this.setCost(currentBestFactor.cost);
            removal.add(this.getMultiplier().get(0));
            objectConditionSet.removeAll(removal);
            this.remainder.selectFactor(objectConditionSet);
        }
    }


    public String createQueryFromExactFactor() {
        if (multiplier.isEmpty()) {
            if (expression != null) {
                return this.expression.createQueryFromPolices();
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
     * Estimates the cost of a guarded representation of a set of policies
     * Selectivity of guard * D * Index access + Selectivity of guard * D * cost of filter * alpha * number of predicates
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     *
     * @return
     */
    public double estimateCostOfGuardRep(ObjectCondition guard, BEExpression partition) {
        long numOfPreds = partition.getPolicies().stream().map(BEPolicy::getObject_conditions).mapToInt(List::size).sum();
        return PolicyConstants.NUMBER_OR_TUPLES * guard.computeL() * (PolicyConstants.IO_BLOCK_READ_COST +
                PolicyConstants.ROW_EVALUATE_COST * 2 * numOfPreds * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED);
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
                double freq = PolicyConstants.NUMBER_OR_TUPLES;
                ObjectCondition gOC = new ObjectCondition();
                for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                    if (!PolicyConstants.INDEXED_ATTRS.contains(oc.getAttribute())) continue;
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
     * Creates a query by AND'ing the guard and partition
     *
     * @param guard
     * @param partition
     * @return
     */
    public String createQueryFromGQ(ObjectCondition guard, BEExpression partition) {
        StringBuilder query = new StringBuilder();
        query.append(guard.print());
        query.append(PolicyConstants.CONJUNCTION);
        query.append("(");
        query.append(partition.createQueryFromPolices());
        query.append(")");
//        System.out.println(query.toString());
        return query.toString();
    }

    public String cleanQueryFromPolices(BEExpression beExpression) {
        StringBuilder query = new StringBuilder();
        String delim = "";
        List<BEPolicy> dupElim = new BEExpression(beExpression.getPolicies()).getPolicies();
        for (int i = 0; i < beExpression.getPolicies().size(); i++) {
            for (int j = i + 1; j < beExpression.getPolicies().size(); j++) {
                BEPolicy bp1 = beExpression.getPolicies().get(i);
                BEPolicy bp2 = beExpression.getPolicies().get(j);
                if (bp1.equalsWithoutId(bp2)) {
                    dupElim.remove(bp1);
                    break;
                }
            }
        }
        for (BEPolicy beP : dupElim) {
            query.append(delim);
            query.append("(" + cleanQueryFromObjectConditions(beP) + ")");
            delim = PolicyConstants.DISJUNCTION;
        }
        return query.toString();
    }

    public String cleanQueryFromObjectConditions(BEPolicy bePolicy) {
        StringBuilder query = new StringBuilder();
        String delim = "";
        List<ObjectCondition> dupElim = new BEPolicy(bePolicy).getObject_conditions();
        for (int i = 0; i < bePolicy.getObject_conditions().size(); i++) {
            for (int j = i + 1; j < bePolicy.getObject_conditions().size(); j++) {
                ObjectCondition oc1 = bePolicy.getObject_conditions().get(i);
                ObjectCondition oc2 = bePolicy.getObject_conditions().get(j);
                if (oc1.equalsWithoutId(oc2)) {
                    dupElim.remove(oc1);
                    break;
                }
            }
        }
        for (ObjectCondition oc : dupElim) {
            query.append(delim);
            query.append(oc.print());
            delim = PolicyConstants.CONJUNCTION;
        }
        return query.toString();
    }

    /**
     * Computes the cost of execution of individual guards and sums them up
     * For the remainder it considers the predicate with highest selectivity as the guard and computes the cost
     * Repeats the cost computation *repetitions* number of times and drops highest and lowest value to smooth it out
     *
     * @return
     */
    public Duration computeGuardCosts() {
        int repetitions = 1;
        Map<ObjectCondition, BEExpression> gMap = getGuardPartitionMapWithRemainder();
        Duration rcost = Duration.ofMillis(0);
//        int numberOfTuples = 0;
//        int totalGuardResultCount = 0;
        for (ObjectCondition kOb : gMap.keySet()) {
            List<Long> cList = new ArrayList<>();
            MySQLResult mySQLResult = new MySQLResult();
//            MySQLResult tempResult = new MySQLResult();
            for (int i = 0; i < repetitions; i++) {
//                tempResult = mySQLQueryManager.runTimedQueryWithResultCount(kOb.print());
//                System.out.println("Count for current guard: " + tempResult.getResultCount());
                mySQLResult = mySQLQueryManager.runTimedQueryWithResultCount(createQueryFromGQ(kOb, gMap.get(kOb)));
//                System.out.println("Count for current guard + partition: " + mySQLResult.getResultCount());
                cList.add(mySQLResult.getTimeTaken().toMillis());
            }
//            totalGuardResultCount += tempResult.getResultCount();
//            numberOfTuples += mySQLResult.getResultCount();
            Collections.sort(cList);
//            List<Long> clippedList = cList.subList(1, repetitions-1);
            Duration gCost = Duration.ofMillis(cList.stream().mapToLong(i -> i).sum() / cList.size());
            rcost = rcost.plus(gCost);
        }
//        System.out.println("total number of tuples with just guard" + totalGuardResultCount);
//        System.out.println("total true number of tuples: " +numberOfTuples);
        return rcost;
    }

    /**
     * Prints the following to a csv file
     * - For each guard
     * - Number of policies in the partition
     * - Number of predicates in policies
     * - Results returned by the guard
     * - Results returned by the guard + partition
     * - Time taken by each guard
     * - Time taken by each guard + partition
     * - Print the guard + partition
     *
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
            int numOfPreds = new BEExpression(gMap.get(kOb).getPolicies()).countNumberOfPredicates();
            guardString.append(numOfPreds);
            guardString.append(",");
            List<Long> gList = new ArrayList<>();
            List<Long> cList = new ArrayList<>();
            int gCount = 0, tCount = 0;
            for (int i = 0; i < repetitions; i++) {
                MySQLResult guardResult = mySQLQueryManager.runTimedQueryWithResultCount(kOb.print());
                if (gCount == 0) gCount = guardResult.getResultCount();
                gList.add(guardResult.getTimeTaken().toMillis());
                MySQLResult completeResult = mySQLQueryManager.runTimedQueryWithResultCount(createQueryFromGQ(kOb, gMap.get(kOb)));
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
            guardString.append(createQueryFromGQ(kOb, gMap.get(kOb)));
            guardResults.add(guardString.toString());
            totalEval = totalEval.plus(gAndPcost);
        }
        System.out.println("Total Guard Evaluation time: " + totalEval);
        guardResults.add("Total Guard Evaluation time," + totalEval.toMillis());
        return guardResults;
    }

    public void printDirtyFilterResults() {
        Map<ObjectCondition, BEExpression> gMap = getGuardPartitionMapWithRemainder();
        int repetitions = 1;
        for (ObjectCondition kOb : gMap.keySet()) {
            System.out.println("Executing Guard " + kOb.print());
            List<Long> gList = new ArrayList<>();
            Duration fTime = Duration.ofMillis(0);
            List<Presence> finalResult = new ArrayList<>();
            for (int i = 0; i < repetitions; i++) {
                MySQLResult guardResult = new MySQLResult();
                guardResult = mySQLQueryManager.runTimedQueryWithResultCount(kOb.print());
                gList.add(guardResult.getTimeTaken().toMillis());
                Instant fsStart = Instant.now();
                finalResult = checkManuallyAgainstPolicieS(guardResult.getQueryResult());
                Instant fsEnd = Instant.now();
                fTime = Duration.between(fsStart, fsEnd);
            }
            Collections.sort(gList);
            System.out.println("Guard Time: " + gList.get(0));
            System.out.println("Filter Time " + fTime.toMillis());
            System.out.println("Size of final result " + finalResult.size());
        }
    }

    private List<Presence> checkManuallyAgainstPolicieS(List<Presence> queryResult) {
        List<Presence> finalResults = new ArrayList<>();
        for (Iterator<Presence> pit = queryResult.iterator(); pit.hasNext(); ) {
            Presence p = pit.next();
            if (Integer.parseInt(p.getTemperature()) >= 58 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 3 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 56 && Integer.parseInt(p.getTemperature()) <= 70) {
                if (Integer.parseInt(p.getEnergy()) >= 56 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 56 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 86 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 56 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 3 && (Integer.parseInt(p.getEnergy()) <= 90)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 64 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 53 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 56 && Integer.parseInt(p.getTemperature()) <= 73) {
                if (Integer.parseInt(p.getEnergy()) >= 33 && (Integer.parseInt(p.getEnergy()) <= 47)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 56 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 32 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 66 && Integer.parseInt(p.getTemperature()) <= 71) {
                if (Integer.parseInt(p.getEnergy()) >= 39 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 66 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 3 && (Integer.parseInt(p.getEnergy()) <= 97)) {
                    finalResults.add(p);
                    continue;
                }
            }
            if (Integer.parseInt(p.getTemperature()) >= 56 && Integer.parseInt(p.getTemperature()) <= 74) {
                if (Integer.parseInt(p.getEnergy()) >= 30 && (Integer.parseInt(p.getEnergy()) <= 77)) {
                    finalResults.add(p);
                    continue;
                }
            }
        }
        return finalResults;
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
     * Guard and paritition map but not including remainder
     *
     * @return
     */
    public HashMap<ObjectCondition, BEExpression> getGuardPartition() {
        if (this.getMultiplier().isEmpty()) return new HashMap<>();
        HashMap<ObjectCondition, BEExpression> gMap = new HashMap<>();
        gMap.put(this.getMultiplier().get(0), this.getQuotient().getExpression());
        if (!this.getRemainder().expression.getPolicies().isEmpty()) {
            gMap.putAll(this.getRemainder().getGuardPartition());
        }
        return gMap;
    }
}