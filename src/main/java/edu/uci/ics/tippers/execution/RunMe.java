package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.data.PolicyExecution;
import edu.uci.ics.tippers.data.PolicyGeneration;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.guard.ExactFactor;
import edu.uci.ics.tippers.model.guard.Factorization;
import edu.uci.ics.tippers.model.policy.BEExpression;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policy5-gen.json"));
        System.out.println(beExpression.createQueryFromPolices());

        Factorization f = new Factorization(beExpression);
        f.approximateFactorization();
        System.out.println(f.getExpression().createQueryFromPolices());

        ExactFactor ef = new ExactFactor(f.getExpression());
        ef.greedyFactorization();
        System.out.println(ef.createQueryFromExactFactor());

//
//        PolicyGeneration pg = new PolicyGeneration();
//        pg.generateBEPolicy(5);

//        Connection conn = DB2ConnectionManager.getInstance().getConnection();


    }
}
