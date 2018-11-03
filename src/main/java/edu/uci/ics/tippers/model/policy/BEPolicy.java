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
    private MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

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
        List<ObjectCondition> contained = this.object_conditions.stream()
                .filter(objCond -> objCond.getType() == oc.getType())
                .filter(objCond -> objCond.compareTo(oc) == 0)
                .collect(Collectors.toList());
        return contained.size() != 0;
    }

    public void deleteObjCond(ObjectCondition oc){
        List<ObjectCondition> toRemove = this.object_conditions.stream()
                .filter(objCond -> objCond.getType() == oc.getType())
                .filter(objCond -> objCond.compareTo(oc) == 0)
                .collect(Collectors.toList());
        if(this.object_conditions.size() == toRemove.size()) return;
        this.object_conditions.removeAll(toRemove);
    }

    /**
     * @return String of the query constructed based on the policy in CNF
     */
    public String createQueryFromObjectConditions(){
        StringBuilder query = new StringBuilder();
        String delim = "";
        BEPolicy dupElim = new BEPolicy(this);
        Set<ObjectCondition> og = new HashSet<>(dupElim.getObject_conditions());
        dupElim.getObject_conditions().clear();
        dupElim.getObject_conditions().addAll(og);
        for (ObjectCondition oc: dupElim.getObject_conditions()) {
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

    /**
     * Estimates the upper bound of the cost of evaluating an individual policy by adding up
     * io block read cost * selectivity of lowest selectivity predicate * D +
     * (D * selectivity of lowest selectivity predicate *  row evaluate cost * 2  * alpha * number of predicates)
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     * @return
     */
    public double estimateCost() {
        ObjectCondition selected = this.getObject_conditions().get(0);
        for (ObjectCondition oc : this.getObject_conditions()) {
            if (PolicyConstants.INDEXED_ATTRS.contains(oc.getAttribute()))
                if (oc.computeL() < selected.computeL())
                    selected = oc;
        }
        double cost = PolicyConstants.NUMBER_OR_TUPLES * selected.computeL() *(PolicyConstants.IO_BLOCK_READ_COST  +
                PolicyConstants.ROW_EVALUATE_COST * 2 * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                        this.getObject_conditions().size());
        return cost;
    }



    //TODO: Change into a non-static method
    /**
     * Selectivity of a conjunctive expression
     * e.g., A = u and B = v
     * sel = set (A) * sel (B)
     * with independence assumption
     * @param objectConditions
     * @return
     */
    public static double computeL(Collection<ObjectCondition> objectConditions){
        double selectivity = 1;
        for (ObjectCondition obj: objectConditions) {
            selectivity *= obj.computeL();
        }
        return selectivity;
    }

    /**
     * Estimates the cost of evaluating an individual policy (for the purpose of extension) by adding up
     * io block read cost * selectivity of predicate on a given attribute * D +
     * (D * selectivity of predicate on a given attribute *  row evaluate cost * 2  * alpha * number of predicates)
     * alpha is a parameter which determines the number of predicates that are evaluated in the policy (e.g., 2/3)
     */
    public double estimateCostForExtension(String attribute) {
        ObjectCondition selected = this.getObject_conditions().stream().filter(o -> o.getAttribute().equals(attribute)).findFirst().get();
        double cost = PolicyConstants.NUMBER_OR_TUPLES * selected.computeL() *(PolicyConstants.IO_BLOCK_READ_COST  +
                PolicyConstants.ROW_EVALUATE_COST * 2 * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                        this.getObject_conditions().size());
        return cost;
    }


    /**
     * Estimates the cost of evaluating an individual policy (for the purpose of selection) rest same as above
     */
    public double estimateCostForSelection(ObjectCondition toBeSelected) {
        double cost = PolicyConstants.NUMBER_OR_TUPLES * toBeSelected.computeL() *(PolicyConstants.IO_BLOCK_READ_COST  +
                PolicyConstants.ROW_EVALUATE_COST * 2 * PolicyConstants.NUMBER_OF_PREDICATES_EVALUATED *
                        this.getObject_conditions().size());
        return cost;
    }

}