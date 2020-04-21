package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.QueryResult;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class SelectGuard {

    //Original expression
    Term input;
    Set<ObjectCondition> canFactors;
    List<Term> finalForm;
    PriorityQueue<Term> allTerms; //Sorted based on utility
    Map<String, BEPolicy> pMap;
    Map<ObjectCondition, Double> costMap;
    Map<BEPolicy, List<Term>> ptMap;

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    public SelectGuard(BEExpression originalExp, boolean extend){
        this.input = new Term();
        this.input.setRemainder(originalExp);
        this.input.setQuotient(originalExp);
        finalForm = new ArrayList<>();
        allTerms = new PriorityQueue<>();
        this.pMap = new HashMap<>();
        this.costMap = new HashMap<>();
        this.ptMap = new HashMap<>();
        houseKeep();
        if(extend){
            GenerateCandidate pm = new GenerateCandidate(this.input.getRemainder());
            pm.extend();
        }
        this.canFactors = collectAllFactors(this.input.getRemainder());
        selectGuards();
    }

    public int numberOfGuards(){
        return finalForm.size();
    }

    /**
     * Backing up original policies by id before they are extended
     */
    private void houseKeep(){
        for (BEPolicy bp: this.input.getRemainder().getPolicies()) {
            pMap.put(bp.getId(), new BEPolicy(bp));
        }
    }

    private Set<ObjectCondition> collectAllFactors(BEExpression originalExp){
        Set<ObjectCondition> pFactors = originalExp.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.INDEX_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        Set<ObjectCondition> pGuards = new HashSet<>();
        for (ObjectCondition pf: pFactors) {
            boolean match = false;
            for (ObjectCondition cf: pGuards) {
                if(pf.equalsWithoutId(cf)) match = true;
            }
            if(!match) pGuards.add(pf);
        }
        return pGuards;
    }


    private double benefit(ObjectCondition factor, BEExpression quotient){
        double ben = 0.0;
        long numPreds = 0;
        for (BEPolicy bp: quotient.getPolicies()) {
            ben += pMap.get(bp.getId()).estimateTableScanCost();
            numPreds += pMap.get(bp.getId()).countNumberOfPredicates();
        }
        return ben - quotient.estimateCPUCost(factor, numPreds);
    }

    private double cost(ObjectCondition factor){
        return PolicyConstants.NUMBER_OR_TUPLES * factor.computeL() * PolicyConstants.IO_BLOCK_READ_COST ;
        //+ PolicyConstants.NUMBER_OR_TUPLES * factor.computeL() * PolicyConstants.ROW_EVALUATE_COST;
    }


    /**
     * Populating costMap, benefitMap, ptMap and allTerms
     */
    private void populating(){
//        System.out.println("Number of candidates: " + this.canFactors.size());
        for (ObjectCondition tempFactor : this.canFactors) {
            BEExpression tempQuotient = new BEExpression(this.input.getRemainder());
            tempQuotient.checkAgainstPolices(tempFactor);
            Term tempTerm = new Term(tempFactor, tempQuotient);
            if(!costMap.containsKey(tempFactor))
                costMap.put(tempFactor, cost(tempFactor));
            tempTerm.setBenefit(benefit(tempFactor, tempQuotient));
            tempTerm.setUtility(tempTerm.getBenefit()/costMap.get(tempTerm.getFactor()));
            for (BEPolicy bp: tempTerm.getQuotient().getPolicies()) {
                if(ptMap.containsKey(bp)){
                    ptMap.get(bp).add(tempTerm);
                }
                else {
                    List<Term> terms = new ArrayList<>();
                    terms.add(tempTerm);
                    ptMap.put(bp, terms);
                }
            }
            allTerms.offer(tempTerm);
        }
    }


    /**
     * Selecting the best guard from allTerms and updating the benefit of related Terms
     */
    private void selectGuards() {
        populating();
        Term mTerm;
        while (!this.allTerms.isEmpty()) {
            mTerm = this.allTerms.poll();
            if (mTerm == null) break;
            if (mTerm.getFactor() == null || mTerm.getQuotient() == null || mTerm.getQuotient().getPolicies().isEmpty())
                break;
            finalForm.add(mTerm);
//            System.out.println(mTerm.getFactor().print());
            List<Term> toUpdate = new ArrayList<>();
            for (BEPolicy bp : mTerm.getQuotient().getPolicies())
                for (Term pTerm : ptMap.get(bp))
                    if(!pTerm.equals(mTerm))
                        toUpdate.add(pTerm);
            for (Term uTerm: toUpdate) {
                allTerms.remove(uTerm);
                uTerm.getQuotient().getPolicies().removeAll(mTerm.getQuotient().getPolicies());
                uTerm.setBenefit(benefit(uTerm.getFactor(), uTerm.getQuotient()));
                uTerm.setUtility(uTerm.getBenefit() / costMap.get(uTerm.getFactor()));
                allTerms.offer(uTerm); //the term has to be removed and added again so that it is inserted in PQ at the right position
            }
        }
    }

    public GuardExp create(String querier, String querier_type){
        List<GuardPart> gps = new ArrayList<>();
        for (Term mt: finalForm) {
            String gpID =  UUID.randomUUID().toString();
            GuardPart gp = new GuardPart();
            gp.setId(gpID);
            gp.setGuard(mt.getFactor());
            gp.setGuardPartition(mt.getQuotient());
            gps.add(gp);
        }
        GuardExp guardExp = new GuardExp();
        guardExp.setGuardParts(gps);
        //TODO: Make it less hardcoded?
        guardExp.setAction(finalForm.get(0).getQuotient().getPolicies().get(0).getAction());
        guardExp.setPurpose(finalForm.get(0).getQuotient().getPolicies().get(0).getPurpose());
        guardExp.setQuerier(querier);
        guardExp.setQuerier_type(querier_type);
        guardExp.setDirty("false");
        guardExp.setLast_updated(new Timestamp(new Date().getTime()));
        return guardExp;
    }

    /**
     * For large policies experiment
     * Same as above except it doesn't include a querier, querier type, or purpose
     * @return
     */
    public GuardExp create(){
        List<GuardPart> gps = new ArrayList<>();
        for (Term mt: finalForm) {
            String gpID =  UUID.randomUUID().toString();
            GuardPart gp = new GuardPart();
            gp.setId(gpID);
            gp.setGuard(mt.getFactor());
            gp.setGuardPartition(mt.getQuotient());
            gps.add(gp);
        }
        GuardExp guardExp = new GuardExp();
        guardExp.setGuardParts(gps);
        guardExp.setAction(finalForm.get(0).getQuotient().getPolicies().get(0).getAction());
        guardExp.setPurpose(finalForm.get(0).getQuotient().getPolicies().get(0).getPurpose());
        guardExp.setDirty("false");
        guardExp.setLast_updated(new Timestamp(new Date().getTime()));
        return guardExp;
    }

    /**
     * (Select * from Presence where G1 and Partition1
     *  UNION Select * from Presence where G2 and Partition2
     *  ....
     *  Select * from Presence where GN and PartitionN)
     * @param noDuplicates
     * @return
     */
    public String createGuardedQuery(boolean noDuplicates){
        List<String> gList = createGuardQueries();
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        for (String g: gList) {
            queryExp.append(delim);
            queryExp.append(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE + g);
            delim = noDuplicates ? PolicyConstants.UNION : PolicyConstants.UNION_ALL;
        }
        return queryExp.toString();
    }



    public List<String> createGuardQueries(){
        List<String> guardQueries = new ArrayList<>();
        for (Term mt: finalForm)
           guardQueries.add(mt.getFactor().print() + PolicyConstants.CONJUNCTION +
                   "(" + mt.getQuotient().createQueryFromPolices() + ")");
        return  guardQueries;
    }

    private String createCleanQueryFromGQ(ObjectCondition guard, BEExpression partition) {
        StringBuilder query = new StringBuilder();
        query.append("USE INDEX (" + PolicyConstants.ATTRIBUTE_IND.get(guard.getAttribute()) + ")");
        query.append(" WHERE ");
        query.append(guard.print());
        partition.removeDuplicates();
        BEExpression queryExp = new BEExpression();
        for (BEPolicy bp: partition.getPolicies()) {
            BEPolicy tp = pMap.get(bp.getId());
            for (ObjectCondition oc: tp.getObject_conditions()){
                if(oc.equalsWithoutId(guard)) {
                    tp.getObject_conditions().remove(oc);
                    break;
                }
            }
            if(!tp.getObject_conditions().isEmpty())
                queryExp.getPolicies().add(tp);
        }
        if(!queryExp.getPolicies().isEmpty()){
            query.append(PolicyConstants.CONJUNCTION);
            query.append("(");
            query.append(queryExp.createQueryFromPolices());
            query.append(")");
        }
        return query.toString();
    }

    private long getOriginalNumPreds(BEExpression beExpression){
        long numPreds = 0;
        for (BEPolicy bp: beExpression.getPolicies()) {
            numPreds += pMap.get(bp.getId()).countNumberOfPredicates();
        }
        return numPreds;
    }

    public List<String> guardAnalysis(int repetitions, boolean execution) {
        List<String> guardResults = new ArrayList<>();
        Duration totalEval = Duration.ofMillis(0);
        int i = 0;
        for (Term mt : finalForm) {
            System.out.println("Guard " + i++ + ": " + mt.getFactor().print());
            StringBuilder guardString = new StringBuilder();
            guardString.append(mt.getQuotient().getPolicies().size());
            guardString.append(",");
            guardString.append(getOriginalNumPreds(mt.getQuotient()));
            guardString.append(",");
            guardString.append(mt.getBenefit());
            guardString.append(",");
            guardString.append(mt.getUtility());
            guardString.append(",");
            if(execution) {
                QueryResult completeResult = mySQLQueryManager.executeQuery(createCleanQueryFromGQ(mt.getFactor(), mt.getQuotient()),
                        false, repetitions);
                QueryResult guardResult = mySQLQueryManager.runTimedQueryWithOutSorting(mt.getFactor().print(), true);
                int gCount = 0, tCount = 0;
                tCount = completeResult.getResultCount();
                gCount = guardResult.getResultCount();

                guardString.append(gCount);
                guardString.append(",");
                guardString.append(tCount);
                guardString.append(",");

                double rCount = 0.0;
                if(tCount != 0 ){
                    rCount = gCount / tCount;
                }
                guardString.append(rCount);
                guardString.append(",");

                guardString.append(completeResult.getTimeTaken().toMillis());
                guardString.append(",");
                totalEval = totalEval.plus(completeResult.getTimeTaken());
            }
            guardString.append(createCleanQueryFromGQ(mt.getFactor(), mt.getQuotient()));
            guardResults.add(guardString.toString());
        }
        if(execution) {
            System.out.println("Total Guard Evaluation time: " + totalEval);
            guardResults.add("Total Guard Evaluation time," + totalEval.toMillis());
        }
        return guardResults;
    }

}
