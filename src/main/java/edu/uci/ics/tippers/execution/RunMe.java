package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.guard.GuardHit;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.io.File;
import java.util.Arrays;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]) {

        String policyDir = PolicyConstants.BE_POLICY_DIR;
        File dir = new File(policyDir);
        File[] policyFiles = dir.listFiles((dir1, name) -> name.toLowerCase().endsWith(".json"));
        Arrays.sort(policyFiles != null ? policyFiles : new File[0]);

        if (policyFiles != null) {
            for (File file : policyFiles) {
                int numOfPolicies = Integer.parseInt(file.getName().replaceAll("\\D+", ""));
                System.out.println(numOfPolicies + " being processed......");
                BEExpression beExpression = new BEExpression();
                beExpression.parseJSONList(Reader.readTxt(policyDir + file.getName()));
                System.out.println(beExpression.getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum());
//                System.out.println(beExpression.createQueryFromPolices());
//                PredicateMerge pm = new PredicateMerge(beExpression);
//                pm.extend();
                System.out.println(beExpression.getPolicies().stream().mapToInt(BEPolicy::countNumberOfPredicates).sum());
//                System.out.println(beExpression.createQueryFromPolices());
                GuardHit gh = new GuardHit(beExpression, true);
            }
        }


//        GroupGeneration groupGeneration = new GroupGeneration();
//        List<UserGroup> userGroups = groupGeneration.readCSVFile("/data/policy_groups.csv");
//        for (UserGroup ug: userGroups) {
//            ug.retrieveMembers();
//            System.out.println(ug.toString());
//        }

//        QueryGeneration qg = new QueryGeneration();
//        boolean [] templates = {false, true, false, false};
//        int numOfQueries = 50;
//        qg.constructWorkload(templates, numOfQueries);

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
