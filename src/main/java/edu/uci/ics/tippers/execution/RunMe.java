package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.data.GroupGeneration;
import edu.uci.ics.tippers.data.QueryGeneration;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.db.MySQLResult;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.manager.GuardPersistor;
import edu.uci.ics.tippers.manager.PolicyPersistor;
import edu.uci.ics.tippers.model.data.UserGroup;
import edu.uci.ics.tippers.model.guard.FactorSearch;
import edu.uci.ics.tippers.model.guard.GuardExp;
import edu.uci.ics.tippers.model.policy.BEExpression;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]) {

//        GroupGeneration groupGeneration = new GroupGeneration();
//        List<UserGroup> userGroups = groupGeneration.readCSVFile("/data/policy_groups.csv");
//        for (UserGroup ug: userGroups) {
//            ug.retrieveMembers();
//            System.out.println(ug.toString());
//        }

        QueryGeneration qg = new QueryGeneration();
        boolean [] templates = {true, false, false, false};
        List<Double> selectivity = new ArrayList<Double>(Arrays.asList(0.00001, 0.00002, 0.00003));
        int numOfQueries = 3;
        List<String> queries = qg.constructWorkload(templates, selectivity, numOfQueries);
        for (String q: queries) {
            System.out.println(q);
            MySQLQueryManager mq = new MySQLQueryManager();
            MySQLResult mqr = mq.runTimedQueryWithResultCount(q);
            System.out.println(mqr.getTimeTaken() + " : " + mqr.getResultCount());
        }

//        DataGeneration dataGeneration = new DataGeneration();
//        dataGeneration.runScript("mysql/schema.sql");
//
//        BEExpression beExpression = new BEExpression();
//        beExpression.parseJSONList(Reader.readFile("/policies/policytestlong.json"));
//
//        FactorSearch fs = new FactorSearch(beExpression);
//        fs.search();
//        GuardExp ge  = fs.create("10001", "user");

//        GuardPersistor gp = new GuardPersistor();
//        gp.insertGuard(ge);
//        System.out.println(gp.retrieveGuard("10001", "user").createQuery());

//        PolicyPersistor persistor = new PolicyPersistor();
//        persistor.insertPolicy(beExpression.getPolicies().get(0));
//        System.out.println(persistor.retrievePolicy("10", "group", null).createQueryFromObjectConditions());

//        User user = new User(10001);
//        user.getUserGroups();

//        for(int i = 0; i <Histogram.getInstance().getBucketMap().get(PolicyConstants.TIMESTAMP_ATTR).size(); i++){
//            System.out.println(Histogram.getInstance().getBucketMap().get(PolicyConstants.TIMESTAMP_ATTR).get(i).toStringEH());
//        }
//
//        Bucket bucket = new Bucket();
//        bucket.setAttribute(PolicyConstants.TIMESTAMP_ATTR);
//        bucket.setLower("2017-03-31 15:09:00.000000");
//        int s = Collections.binarySearch(Histogram.getInstance().getBucketMap().get(PolicyConstants.TIMESTAMP_ATTR), bucket);
//        System.out.println(-s);
//        System.out.println(Histogram.getInstance().getBucketMap().get(PolicyConstants.TIMESTAMP_ATTR).get(-s -1 ).toStringEH());
//
//        BEExpression beExpression = new BEExpression();
//        beExpression.parseJSONList(Reader.readFile("/policies/policyfs1.json"));
//        System.out.println(beExpression.createQueryFromPolices());
//
//        FactorSearch fs = new FactorSearch(beExpression);
//        fs.search();

//        FactorExtension gg = new FactorExtension(beExpression);
//        gg.doYourThing();
//        System.out.println(gg.getGenExpression().createQueryFromPolices());
//
//        FactorSelection tt = new FactorSelection(gg.getGenExpression());
//        tt.selectGuards();
//        System.out.println(tt.createQueryFromExactFactor());

//        ExactFactorization ef = new ExactFactorization();
//        ef.memoize(beExpression);
//        ef.printfMap();

//        PredicateExtensionOld f = new PredicateExtensionOld(beExpression);
//        f.approximateFactorization();
//        System.out.println(f.getExpression().createQueryFromPolices());
//
//        NaiveExactFactorization ef = new NaiveExactFactorization(f.getExpression());
//        ef.greedyFactorization();
//        System.out.println(ef.createQueryFromExactFactor());

//
//        PolicyGeneration pg = new PolicyGeneration();
//        pg.generateBEPolicy(5);

    }
}
