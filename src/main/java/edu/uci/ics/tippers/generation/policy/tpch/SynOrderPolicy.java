package edu.uci.ics.tippers.generation.policy.tpch;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLConnectionManager;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.tpch.OrderProfile;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SynOrderPolicy {

    TPolicyGen tpg;
    PolicyPersistor polper;
    private Connection connection;
    Random r;

    private final double INDIVIDUAL_CHANCE =  0.7;
    private final double DATE_CHANCE = 0.75;
    private final double PRICE_CHANCE = 0.7;
    private final double CLERK_CHANCE = 0.6;
    private final double PRIORITY_CHANCE = 0.5;
    private static final int PRIORITIES_PER_QUERIER = 2;

    private final int MIN_POLICIES = 50;
    private final int MAX_POLICIES = 400;
    private final int POLICY_MEAN = 200;
    private final int POLICY_STD = 30;

    private static final double TOTAL_PRICE_STD = 88621.40;
    private static final double TOTAL_PRICE_AVG = 151219.53;

    private final double MAX_TOTAL_PRICE;
    private final double MIN_TOTAL_PRICE;
    private final LocalDate MAX_DATE;
    private final LocalDate MIN_DATE;

    public static final List<String> ORDER_PROFILES = Stream.of(OrderProfile.values()).map(OrderProfile::getPriority).collect(Collectors.toList());

    public SynOrderPolicy(){

        tpg = new TPolicyGen();
        polper = PolicyPersistor.getInstance();
        connection = MySQLConnectionManager.getInstance().getConnection();
        r = new Random();

        MAX_TOTAL_PRICE = tpg.getTotalPrice("MAX");
        MIN_TOTAL_PRICE = tpg.getTotalPrice("MIN");
        MAX_DATE = tpg.getOrderDate("MAX").toLocalDateTime().toLocalDate();
        MIN_DATE = tpg.getOrderDate("MIN").toLocalDateTime().toLocalDate();

    }

    public void generatePolicies(double overlap){
        List<BEPolicy> synPolicies = new ArrayList<>();
        List<Integer> allCustomers = tpg.getAllCustomerKeys();

        for (int querier: allCustomers) {
            //Selecting number of policies
            int numOfPolicies = (int) Math.min(MIN_POLICIES + r.nextGaussian() * POLICY_STD + POLICY_MEAN, MAX_POLICIES);
            //Selecting the list of owners (Number of Policies - Number of Policies * overlap)
            List<Integer> owners = new ArrayList<>(allCustomers);
            owners.remove(new Integer(querier)); // removing the querier as an owner in the policy
            Collections.shuffle(owners);
            owners = owners.subList(0, (int) (numOfPolicies - numOfPolicies*overlap));
            //Selecting profile for the user
            String profile = ORDER_PROFILES.get(r.nextInt(ORDER_PROFILES.size()));
            //Selecting priority for the user
            List<String> priorties = new ArrayList<>(ORDER_PROFILES);
            Collections.shuffle(priorties);
            priorties = priorties.subList(0, PRIORITIES_PER_QUERIER);
            //Selecting delta for price predicate
            double price_delta = (MAX_TOTAL_PRICE - MIN_TOTAL_PRICE) * overlap;
            //Selecting delta for date predicate
            long days = ChronoUnit.DAYS.between(MIN_DATE, MAX_DATE);
            long day_delta = (long) (overlap * days);
            //Selecting the clerk
            String clerk = String.valueOf(r.nextInt(10));
            int cnt = 0;
            for (int i = 0; i < numOfPolicies; i++) {
                int ownerPred = Math.random() < INDIVIDUAL_CHANCE? owners.get(r.nextInt(owners.size())): 0;
                double priceSeed = r.nextGaussian() * TOTAL_PRICE_STD + TOTAL_PRICE_AVG;
                PricePredicate pricePred = Math.random() < PRICE_CHANCE?
                        new PricePredicate(priceSeed, Math.min(priceSeed + price_delta, MAX_TOTAL_PRICE)): null;
                LocalDate randomDate = MIN_DATE.plusDays(ThreadLocalRandom.current().nextLong(days+1));
                DatePredicate datePred = Math.random() < DATE_CHANCE? new DatePredicate(randomDate, day_delta): null;
                String clerkPred = Math.random() < CLERK_CHANCE? clerk: null;
                String priorityPred = Math.random() < PRIORITY_CHANCE? priorties.get(r.nextInt(priorties.size())): null;
                synPolicies.add(tpg.generatePolicies(querier, ownerPred, clerkPred, null, pricePred, datePred,
                        priorityPred, PolicyConstants.ACTION_ALLOW));
            }
            polper.insertPolicy(synPolicies);
            break; //TODO: REMOVE THIS!!!!!!!
        }
    }

    public static void main(String [] args) {
        SynOrderPolicy sop = new SynOrderPolicy();
        sop.generatePolicies(0.005);
    }
}
