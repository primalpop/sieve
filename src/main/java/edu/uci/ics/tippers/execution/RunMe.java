package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.db.DB2ConnectionManager;
import edu.uci.ics.tippers.fileop.Reader;
import edu.uci.ics.tippers.model.guard.ExactFactor;
import edu.uci.ics.tippers.model.policy.BEExpression;

import java.sql.Connection;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policy2.json"));


        System.out.println(beExpression.createQueryFromPolices());




//        ExactFactor ef = new ExactFactor(beExpression);
//        ef.greedyFactorization();
//        System.out.println(ef.createQueryFromExactFactor());


//        Connection conn = DB2ConnectionManager.getInstance().getConnection();


    }
}
