package model.guard;

import common.PolicyConstants;
import db.MySQLQueryManager;
import model.policy.ObjectCondition;

import java.util.List;

/**
 * Created by cygnus on 10/29/17.
 */
public class ExactFactor extends  Factor {

    public void ExactFactor(){
    }

    /**
     *
     * E = D.Q + R
     *
     */
    @Override
    public void factorize() {

        

    }

    /**
     * 
     * @return
     */

    @Override
    public long computeCost(List<ObjectCondition> objectConditions){
        StringBuilder query = new StringBuilder();
        MySQLQueryManager queryManager = new MySQLQueryManager();
        String delim = "";
        for (ObjectCondition oc:  objectConditions) {
            query.append(delim);
            query.append(oc.print());
            delim = PolicyConstants.CONJUNCTION;
        }
        return queryManager.runTimedQuery(query.toString());
    }


    @Override
    public double computeFalsePositives() {
        StringBuilder query = new StringBuilder();
        return 0;
    }
}
