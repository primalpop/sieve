package model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.PolicyConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by cygnus on 9/25/17.
 */
public class BEPolicy implements Comparable<BEPolicy> {

    @JsonProperty("id")
    private String id;

    @JsonProperty("description")
    private String description;

    @JsonProperty("metadata")
    private String metadata;

    /**
     * 1) user = Alice ^ loc = 2065
     * 2) user = Bob ^ time > 5pm
     * 3) user = Alice ^ 5 pm < time < 7 pm
     */
    @JsonProperty("object_conditions")
    private List<ObjectCondition> object_conditions;

    /**
     * 1) querier = John
     * 2) querier = John ^ time = 6 pm
     * 3) querier = John ^ location = 2065
     */
    @JsonProperty("querier_conditions")
    private List<QuerierCondition> querier_conditions;

    /**
     * 1) Analysis
     * 2) Concierge
     * 3) Noodle
     */
    @JsonProperty("purpose")
    private String purpose;

    /**
     * 1) Allow
     * 2) Deny
     */
    @JsonProperty("action")
    private String action;

    public BEPolicy(){

    }

    public BEPolicy(BEPolicy bePolicy){
        this.id = bePolicy.getId();
        this.description = bePolicy.getDescription();
        this.metadata = bePolicy.getMetadata();
        this.object_conditions = new ArrayList<ObjectCondition>(bePolicy.getObject_conditions().size());
        for(ObjectCondition oc: bePolicy.getObject_conditions()){
            this.object_conditions.add(new ObjectCondition(oc));
        }
        this.querier_conditions = new ArrayList<QuerierCondition>(bePolicy.getQuerier_conditions().size());
        for(QuerierCondition qc: bePolicy.getQuerier_conditions()){
            this.querier_conditions.add(new QuerierCondition(qc));
        }
        this.action = bePolicy.getAction();
        this.purpose = bePolicy.getPurpose();

    }

    public BEPolicy(String id, String description, List<ObjectCondition> object_conditions, List<QuerierCondition> querier_conditions, String purpose, String action) {
        this.id = id;
        this.description = description;
        this.object_conditions = object_conditions;
        this.querier_conditions = querier_conditions;
        this.purpose = purpose;
        this.action = action;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public List<ObjectCondition> getObject_conditions() {
        return object_conditions;
    }

    public void setObject_conditions(List<ObjectCondition> object_conditions) {
        this.object_conditions = object_conditions;
    }

    public List<QuerierCondition> getQuerier_conditions() {
        return querier_conditions;
    }

    public void setQuerier_conditions(List<QuerierCondition> querier_conditions) {
        this.querier_conditions = querier_conditions;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public List<String> objectConditionAttributes(){
        List<String> attrs = new ArrayList<String>();
        for(ObjectCondition oc: object_conditions) {
            if(!attrs.contains(oc.getAttribute())){
                attrs.add(oc.getAttribute());
            }
        }
        return attrs;
    }


    public static BEPolicy parseJSONObject(String jsonData){
        ObjectMapper objectMapper = new ObjectMapper();
        BEPolicy bePolicy = null;
        try {
            bePolicy = objectMapper.readValue(jsonData, BEPolicy.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bePolicy;
    }

    //TODO: Make it a single method for both subclasses taking parent class as parameter

    public String serializeObjectConditions(List<ObjectCondition> bcs){
        StringBuilder result = new StringBuilder();
        result.append("[ ");
        for(int i = 0; i < bcs.size(); i++){
            result.append(bcs.get(i).print());
        }
        result.append(" ],");
        return result.toString();
    }

    public String serializeQuerierConditions(List<QuerierCondition> bcs){
        StringBuilder result = new StringBuilder();
        result.append("[ ");
        for(int i = 0; i < bcs.size(); i++){
            result.append(bcs.get(i).print());
        }
        result.append(" ],");
        return result.toString();
    }

    public void print(){
        System.out.println(
                "Policy ID: " + this.getId() +
                        " Description: " + this.getDescription() +
                        " Object Conditions: " + serializeObjectConditions(this.getObject_conditions()) +
                        " Querier Conditions: " + serializeQuerierConditions(this.getQuerier_conditions()) +
                        " Action: " + this.getAction() +
                        " Purpose: " + this.getPurpose()
        );
    }

    /**
     * Check if a boolean condition is contained within the policy
     * @param oc
     * @return
     */
    public boolean containsObjCond(ObjectCondition oc){
        List<ObjectCondition> ocs = this.getObject_conditions();
        for (int i = 0; i < ocs.size(); i++) {
            if(oc.getAttribute().equals(ocs.get(i).getAttribute())){
                if(oc.getType().equals(ocs.get(i).getType())){
                    if(oc.compareBooleanPredicates(ocs.get(i).getBooleanPredicates())){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void deleteObjCond(ObjectCondition oc){
        List<ObjectCondition> toRemove = this.object_conditions.stream()
                .filter(objCond -> objCond.compareTo(oc) == 0)
                .collect(Collectors.toList());
        this.object_conditions.removeAll(toRemove);
    }

    public String createQueryFromObjectConditions(){
        StringBuilder query = new StringBuilder();
        String delim = "";
        for (int i = 0; i < this.object_conditions.size(); i++) {
            ObjectCondition oc = this.object_conditions.get(i);
            query.append(delim);
            query.append(oc.print());
            delim = PolicyConstants.CONJUNCTION;
        }
        return query.toString();

    }

    /**
     * TODO: Check rest of the policy parameters, currently only check policy id and object conditions
     * @param bePolicy
     * @return
     */
    @Override
    public int compareTo(BEPolicy bePolicy) {
        return this.getId().equals(bePolicy.getId()) && compareBooleanConditions(bePolicy.getObject_conditions())? 0: -1;
    }

    public boolean compareBooleanConditions(List<ObjectCondition> objectConditions){
        if (objectConditions.size() != this.getObject_conditions().size())
            return false;
        int count = 0;
        for (int i = 0; i < objectConditions.size(); i++) {
            for (int j = 0; j < this.getObject_conditions().size() ; j++) {
                if (objectConditions.get(i).compareTo(this.getObject_conditions().get(j)) == 0){
                    count++;
                }
            }
        }
        if (count == objectConditions.size()) return true;
        return false;
    }
}
