package edu.uci.ics.tippers.generation.query.tpch;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.generation.policy.tpch.DatePredicate;
import edu.uci.ics.tippers.generation.policy.tpch.TPolicyGen;
import edu.uci.ics.tippers.generation.query.QueryGen;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TPCHQueryGeneration extends QueryGen {

    private List<Integer> cust_keys;
    private List<String> order_clerks;
    private List<String> order_priorities;
    Timestamp date_beg, date_end;
    private List<Integer> numMonths;
    private List<Integer> numCustomers;
    private List<Integer> numClerks;

    TPolicyGen tpg;

    public TPCHQueryGeneration(){
        tpg = new TPolicyGen();
        this.cust_keys = tpg.getAllCustomerKeys();
        this.order_clerks = tpg.getAllClerks();
        this.date_beg = tpg.getOrderDate("MIN");
        this.date_end = tpg.getOrderDate("MAX");
        this.order_priorities = tpg.getAllProfiles();
        numMonths = new ArrayList<>(Arrays.asList(2, 4, 8, 12, 16, 20, 24, 28, 32, 34, 38, 42, 46, 50));
        numCustomers = new ArrayList<Integer>(Arrays.asList(1000, 2000, 3000, 5000, 10000, 15000, 20000, 30000, 35000, 40000, 50000));
        numClerks = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 5, 10, 15, 20, 30, 50, 100, 200));
    }

    /**
     * Select * from ORDERS where O_CLERK in [......]
     * and O_ORDERDATE >= d1 and O_ORDERDATE <= d2
     * @param selTypes
     * @param numOfQueries
     * @return
     */
    @Override
    public List<QueryStatement> createQuery1(List<String> selTypes, int numOfQueries) {
        List<QueryStatement> queries = new ArrayList<>();
        int i = 0, j = 0;
        for (int k = 0; k < selTypes.size(); k++) {
            int numQ = 0;
            int clerks = numClerks.get(i);
            int monthOffset = this.numMonths.get(j);
            boolean clerkFlag = true;
            String selType = selTypes.get(k);
            double chosenSel = getSelectivity(selType);
            System.out.println("Chosen Selectivity " + chosenSel);
            do {
                DatePredicate datePredicate = new DatePredicate(this.date_beg.toLocalDateTime().toLocalDate(), monthOffset);
                String query = String.format(PolicyConstants.ORDER_DATE + " >= \"%s\" AND " + PolicyConstants.ORDER_DATE +
                                "<= \"%s\" ", datePredicate.getStartDate().toString(), datePredicate.getEndDate().toString());
                List<String> clerkPreds = new ArrayList<>();
                int predCount = 0;
                while (predCount < clerks) {
                    clerkPreds.add(String.valueOf(order_clerks.get(new Random().nextInt(order_clerks.size()))));
                    predCount += 1;
                }
                query += "AND O_CLERK in (";
                query += order_clerks.stream().map(item -> "\"" + item + "\", ").collect(Collectors.joining(" "));
                query = query.substring(0, query.length() - 2); //removing the extra comma
                query += ")";
                float selQuery = queryManager.checkSelectivity(query);
                String querySelType = checkSelectivityType(chosenSel, selQuery);
                if(querySelType == null){
                    if(selType.equalsIgnoreCase("low") || selType.equalsIgnoreCase("medium")) {
                        if (clerkFlag && ++i < numClerks.size()) {
                            clerks = numClerks.get(i);
                            clerkFlag = false;
                        } else if (!clerkFlag && ++j < numMonths.size()) {
                            monthOffset = numMonths.get(j);
                            clerkFlag = true;
                        }
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    System.out.println("Adding query with " + querySelType + " selectivity");
                    queries.add(new QueryStatement(query, 1, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                    chosenSel = getSelectivity(selType);
                    System.out.println("Chosen Selectivity " + chosenSel);
                    continue;
                }
                if (selType.equalsIgnoreCase("low")){
                    if (querySelType.equalsIgnoreCase("medium")
                            || querySelType.equalsIgnoreCase("high")){
                        monthOffset = Math.max(1, monthOffset-1);
                    }
                }
                else if (selType.equalsIgnoreCase("medium")) { //TODO: Is this required?
                    if(querySelType.equalsIgnoreCase("low")) {
                        if (clerkFlag && ++i < numClerks.size()) {
                            clerks = numClerks.get(i);
                            clerkFlag = false;
                        } else if (!clerkFlag && ++j < numMonths.size()) {
                            monthOffset = numMonths.get(j);
                            clerkFlag = true;
                        }
                    }
                    else if(querySelType.equalsIgnoreCase("high")){
                        monthOffset = Math.max(1, monthOffset-1);
                    }
                }
                else {
                    if (querySelType.equalsIgnoreCase("low")
                            || querySelType.equalsIgnoreCase("medium")){
                        if (clerkFlag && ++i < numClerks.size()) {
                            clerks = numClerks.get(i);
                            clerkFlag = false;
                        } else if (!clerkFlag && ++j < numMonths.size()) {
                            monthOffset = numMonths.get(j);
                            clerkFlag = true;
                        }
                    }
                }
            } while (numQ < numOfQueries);
        }
        return queries;
    }

    /**
     * Query 2: Select * from ORDERS where O_CUSTKEY in [......]
     * and O_TOTALPRICE >= p1 and O_TOTALPRICE <= p2
     * @param selTypes
     * @param numOfQueries
     * @return
     */
    @Override
    public List<QueryStatement> createQuery2(List<String> selTypes, int numOfQueries) {
        return null;
    }

    @Override
    public List<QueryStatement> createQuery3(List<String> selTypes, int numOfQueries) {
        return null;
    }

    @Override
    public List<QueryStatement> createQuery4() {
        return null;
    }
}
