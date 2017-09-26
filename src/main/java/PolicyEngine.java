import model.acp.BEPolicy;
import model.acp.ObjectCondition;
import model.acp.QuerierCondition;
import model.acp.RelOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cygnus on 9/25/17.
 */
public class PolicyEngine {

    public static void main(String args[]){


        ObjectCondition oc = new ObjectCondition("location", RelOperator.EQUALS, "2065");
        List<ObjectCondition> ocs = new ArrayList<ObjectCondition>();
        ocs.add(oc);

        QuerierCondition qc = new QuerierCondition("name", RelOperator.EQUALS, "John");
        List<QuerierCondition> qcs = new ArrayList<QuerierCondition>();
        qcs.add(qc);

        BEPolicy samplePolicy = new BEPolicy("abcd", "Sample Policy", ocs, qcs, "Concierge", "Analysis");

    }
}
