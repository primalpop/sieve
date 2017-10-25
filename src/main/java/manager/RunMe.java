package manager;

import model.policy.BEPolicy;
import model.policy.ObjectCondition;
import model.policy.QuerierCondition;
import model.policy.RelOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class RunMe {

    public static void main(String args[]){


        /**
         * Testing creation of policy
         */

        ObjectCondition oc = new ObjectCondition("location", RelOperator.EQUALS, "2065");
        List<ObjectCondition> ocs = new ArrayList<ObjectCondition>();
        ocs.add(oc);

        QuerierCondition qc = new QuerierCondition("name", RelOperator.EQUALS, "John");
        List<QuerierCondition> qcs = new ArrayList<QuerierCondition>();
        qcs.add(qc);

        BEPolicy samplePolicy = new BEPolicy("abcd", "Sample Policy", ocs, qcs, "Concierge", "Analysis");

        /**
         * Testing Query rewriting
         */

        System.out.println(manager.QueryRewrite.rewriteQuery(0,2,0, false));

    }
}
