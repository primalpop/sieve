package edu.uci.ics.tippers.model.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.guavamini.Lists;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.db.MySQLQueryManager;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by cygnus on 9/25/17.
 */
public class BEPolicy {

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
        this.object_conditions = new ArrayList<ObjectCondition>();
        this.querier_conditions = new ArrayList<QuerierCondition>();
    }

    public BEPolicy(BEPolicy bePolicy){
        this.id = bePolicy.getId();
        this.description = bePolicy.getDescription();
        this.metadata = bePolicy.getMetadata();
        this.object_conditions = new ArrayList<ObjectCondition>(bePolicy.getObject_conditions().size());
        for(ObjectCondition oc: bePolicy.getObject_conditions()){
            this.object_conditions.add(new ObjectCondition(oc));
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

    public BEPolicy(List<ObjectCondition> objectConditions){

        this.object_conditions = objectConditions;
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

    public List<String> retrieveObjCondAttributes(){
        Set<String> attrs = new HashSet<>();
        for(ObjectCondition oc: object_conditions) {
            attrs.add(oc.getAttribute());
        }
        return Lists.newArrayList(attrs);
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
     * Check if a set of object conditions is contained in a policy
     * @param objectConditionSet
     * @return true if all object conditions are contained in the policy, false otherwise
     */
    public boolean containsObjCond(Set<ObjectCondition> objectConditionSet){
       return Sets.newHashSet(this.object_conditions).containsAll(objectConditionSet);
    }

    public boolean containsObjCond(ObjectCondition oc){
        return this.object_conditions.stream()
                .filter(objCond -> objCond.getType() == oc.getType())
                .anyMatch(objCond -> objCond.compareTo(oc) == 0);
    }

    public void deleteObjCond(ObjectCondition oc){
        List<ObjectCondition> toRemove = this.object_conditions.stream()
                .filter(objCond -> objCond.getType() == oc.getType())
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BEPolicy bePolicy = (BEPolicy) o;
        return Objects.equals(id, bePolicy.id) &&
                Objects.equals(new HashSet<>(object_conditions), new HashSet<>(bePolicy.object_conditions));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, object_conditions);
    }

    /**
     * Calculates the powerset of the object conditions of the policy which includes empty set and complete set
     * @return Set of object condition sets
     */
    public Set<Set<ObjectCondition>> calculatePowerSet() {
        Set<ObjectCondition> objectConditionSet = new HashSet<>(this.getObject_conditions());
        Set<Set<ObjectCondition>> result =  Sets.powerSet(objectConditionSet);
        return result;
    }

    public long computeCost(){
        return MySQLQueryManager.runTimedQuery(createQueryFromObjectConditions());
    }


}
