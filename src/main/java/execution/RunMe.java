package execution;

import fileop.Reader;
import model.guard.ExactFactor;
import model.policy.BEExpression;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policy2.json"));

        ExactFactor ef = new ExactFactor(beExpression);
        ef.findBestFactor();
        System.out.println(ef.createQueryFromExactFactor());
    }
}
