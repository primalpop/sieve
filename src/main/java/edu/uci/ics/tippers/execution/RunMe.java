package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.guard.ExactFactor;
import edu.uci.ics.tippers.model.policy.BEExpression;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policy2.json"));

        ExactFactor ef = new ExactFactor(beExpression);
        ef.greedyFactorization();
        System.out.println(ef.createQueryFromExactFactor());
    }
}
