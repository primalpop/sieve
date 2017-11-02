package model.policy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.PolicyConstants;
import db.MySQLQueryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by cygnus on 11/1/17.
 */
public class BEExpression {

    MySQLQueryManager queryManager = new MySQLQueryManager();


    List<BEPolicy> policies;

    public BEExpression(){
        this.policies = new ArrayList<BEPolicy>();

    }

    public BEExpression(List<BEPolicy> policies){
        this.policies = new ArrayList<BEPolicy>(policies);
    }

    public BEExpression(BEExpression beExpression){
        this.policies = new ArrayList<BEPolicy>(beExpression.getPolicies());
    }

    public List<BEPolicy> getPolicies() {
        return policies;
    }

    public void setPolicies(List<BEPolicy> policies) {
        this.policies = policies;
    }



    /**
     * Get all the object conditions from a list of policies
     * @return
     */
    public List<ObjectCondition> getAll(){
        List<ObjectCondition> objConds = new ArrayList<ObjectCondition>();
        for (int i = 0; i < this.policies.size(); i++) {
            List<ObjectCondition> policyCond = this.policies.get(i).getObject_conditions();
            for (int j = 0; j < policyCond.size(); j ++){
                objConds.add(policyCond.get(j));

            }
        }
        return objConds;
    }

    /**
     * Get the unique set of object conditions from a list of policies
     * Currently only identifies a single object condition as factor <-- to be extended to multiple object condition
     * @return
     */
    public List<ObjectCondition> getUnique(){
        List<ObjectCondition> objConds = new ArrayList<ObjectCondition>();
        Set<ObjectCondition> objSet = new HashSet<ObjectCondition>(this.getAll());
        objConds.clear();
        objConds.addAll(objSet);
        return objConds;
    }

    /**
     * Get the repeating set of object conditions from a list of policies
     * Currently only identifies a single object condition as factor <-- to be extended to multiple object condition
     * @return
     */
    public List<ObjectCondition> getRepeating() {
        final Set<ObjectCondition> setToReturn = new HashSet();
        List<ObjectCondition> objConds = new ArrayList<ObjectCondition>();
        final Set<ObjectCondition> set = new HashSet();

        for (ObjectCondition objC : this.getAll()) {
            if (!set.add(objC)) {
                setToReturn.add(objC);
            }
        }
        objConds.addAll(setToReturn);
        return objConds;
    }



    /**
     * Given a predicate and list of policies, identify the list of policies containing that predicate
     * @param objectCondition
     * @return
     */
    public List<BEPolicy> checkAgainstPolicies(ObjectCondition objectCondition){
        List<BEPolicy> polConPred = new ArrayList<BEPolicy>();
        for (int i = 0; i < this.policies.size(); i++) {
            if(this.policies.get(i).containsObjCond(objectCondition))
                polConPred.add(this.policies.get(i));
        }
        return polConPred;
    }

    /**
     * Deletes the given predicate from the policies that contain it
     * @param objectCondition
     * @return
     */
    public List<BEPolicy> removeFromPolicies(ObjectCondition objectCondition){
        List<BEPolicy> polRemPred = new ArrayList<BEPolicy>(this.policies);
        for (int i = 0; i < this.policies.size(); i++) {
            polRemPred.get(i).deleteObjCond(objectCondition);
        }
        return polRemPred;
    }

    /**
     * Given a list of policies, it creates a query by
     * AND'ing object conditions that belong to the same policy
     * and by OR'ing the object conditions across policies
     * @return
     */
    public String createQueryFromPolices(){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (int i = 0; i < this.policies.size(); i++) {
            query.append(delim);
            query.append(this.policies.get(i).createQueryFromObjectConditions());
            delim = PolicyConstants.DISJUNCTION;
        }
        return query.toString();
    }


    public long computeCost(){
        return queryManager.runTimedQuery(createQueryFromPolices());
    }

    public double computeFalsePositives() {
        return queryManager.runCountingQuery(createQueryFromPolices());
    }

    public void parseJSONList(String jsonData) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<BEPolicy> bePolicies = null;
        try {
            bePolicies = objectMapper.readValue(jsonData, new TypeReference<List<BEPolicy>>(){});
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.policies = bePolicies;
    }
}
