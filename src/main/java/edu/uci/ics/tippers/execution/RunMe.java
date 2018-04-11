package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.guard.ApproxFactorization;
import edu.uci.ics.tippers.model.guard.ExactFactorization;
import edu.uci.ics.tippers.model.guard.GreedyExact;
import edu.uci.ics.tippers.model.guard.NaiveExactFactorization;
import edu.uci.ics.tippers.model.policy.BEExpression;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]) {

        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policy2.json"));
        System.out.println(beExpression.createQueryFromPolices());

        GreedyExact gf = new GreedyExact(beExpression);
        gf.GFactorize();
        System.out.println(gf.createQueryFromExactFactor());


//        ExactFactorization ef = new ExactFactorization();
//        ef.memoize(beExpression);
//        ef.printfMap();

//        ApproxFactorization f = new ApproxFactorization(beExpression);
//        f.approximateFactorization();
//        System.out.println(f.getExpression().createQueryFromPolices());
//
//        NaiveExactFactorization ef = new NaiveExactFactorization(f.getExpression());
//        ef.greedyFactorization();
//        System.out.println(ef.createQueryFromExactFactor());

//
//        PolicyGeneration pg = new PolicyGeneration();
//        pg.generateBEPolicy(5);

//        Connection conn = DB2ConnectionManager.getInstance().getConnection();


    }
}
