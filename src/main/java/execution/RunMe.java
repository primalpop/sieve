package execution;

import common.PolicyConstants;
import model.guard.ExactFactor;
import model.policy.BEPolicy;
import model.policy.ObjectCondition;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){

        List<BEPolicy> policies = BEPolicy.parseJSONList(readFile("/policies/policy1.json"));

//        System.out.println(policies.get(0).getObject_conditions().get(0).print());
//
//        System.out.println(policies.get(1).getObject_conditions().get(0).print());
//
//        System.out.println(policies.get(0).getObject_conditions().get(0).checkSame(policies.get(1).getObject_conditions().get(0)));
//
//        System.out.println(policies.get(0).getObject_conditions().get(0).checkOverlap(policies.get(1).getObject_conditions().get(0)));
        
//        ExactFactor ef = new ExactFactor();
//
//        List<ObjectCondition> dnf = policies.get(0).getObject_conditions();
//        dnf.addAll(policies.get(1).getObject_conditions());
//
//        List<String> combiners = new ArrayList<String>();
//        combiners.add(PolicyConstants.CONJUNCTION);
//        combiners.add(PolicyConstants.DISJUNCTION);
//        combiners.add(PolicyConstants.CONJUNCTION);
//
//        println(ef.computeCost(dnf, combiners));
//
//        ef.factorize(dnf, combiners);


        ExactFactor ef = new ExactFactor();


    }

    public static String readFile(String filename) {
        String result = "";
        try {
            InputStream is = RunMe.class.getResourceAsStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void println(Object line) {
        System.out.println(line);
    }

}
