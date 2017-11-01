package execution;

import fileop.Reader;
import model.guard.ExactFactor;
import model.policy.BEExpression;
import model.policy.BEPolicy;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        BEExpression beExpression = new BEExpression();
        beExpression.parseJSONList(Reader.readFile("/policies/policy1.json"));

        ExactFactor ef = new ExactFactor(beExpression);
        ef.factorize();

        System.out.println(ef.createQueryFromExactFactor());
    }
}
