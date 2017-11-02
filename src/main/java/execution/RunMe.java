package execution;

import fileop.Reader;
import manager.Generator;
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
        beExpression.parseJSONList(Reader.readFile("/policies/policy2.json"));

        Generator generator = new Generator();
        ExactFactor ef = generator.generateGuard(beExpression);
        System.out.println(ef.createQueryFromExactFactor());
    }
}
