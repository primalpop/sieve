package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.manager.Persistor;
import edu.uci.ics.tippers.model.guard.FactorSearch;
import edu.uci.ics.tippers.model.policy.BEExpression;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]) {

//        DataGeneration dataGeneration = new DataGeneration();
//        dataGeneration.runScript("mysql/schema.sql");

//        BEExpression beExpression = new BEExpression();
//        beExpression.parseJSONList(Reader.readFile("/policies/deprecated/policyext2.json"));
//
//        Persistor persistor = new Persistor();
//        persistor.insertPolicy(beExpression.getPolicies().get(0));

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
        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policyext2.json"));
        System.out.println(beExpression.createQueryFromPolices());

        FactorSearch fs = new FactorSearch(beExpression);
        System.out.println(fs.search());

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
