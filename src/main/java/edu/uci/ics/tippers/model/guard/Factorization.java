package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by cygnus on 2/14/18.
 */
public class Factorization {

    MySQLQueryManager queryManager = new MySQLQueryManager();

    //Original expression
    BEExpression expression;

    // Chosen multiplier
    List<ObjectCondition> multiplier;

    // Polices from the original expression that contain the multiplier
    Factorization quotient;

    // Polices from the original expression that does not contain the multiplier
    Factorization reminder;

    //Cost of evaluating the expression
    Long cost;

    public Factorization(){
        this.expression = new BEExpression();
        this.multiplier = new ArrayList<ObjectCondition>();
        this. cost = PolicyConstants.INFINTIY;
    }

    public Factorization(BEExpression expression){
        this.expression = new BEExpression(expression);
        this.multiplier = new ArrayList<ObjectCondition>();
        this.cost = PolicyConstants.INFINTIY;
    }


    public static Calendar timestampStrToCal(String timestamp) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(PolicyConstants.TIMESTAMP_FORMAT);
        try {
            cal.setTime(sdf.parse(timestamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return cal;
    }

    private long computeL(BEPolicy bePolicy){
        String query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " +  bePolicy.createQueryFromObjectConditions();
        return queryManager.runCountingQuery(query);
    }

    private long computeL(ObjectCondition objectCondition){
        String query = PolicyConstants.SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS + " where " +  objectCondition.print();
        return queryManager.runCountingQuery(query);

    }

    /**
     * Computing incremental function F of Policy of a single predicate of the form axyz to a(xyz)
     * F(ax) = -l(x) + l(ax)
     * @param original
     * @param factorized
     * @return
     */
    private long computeF(BEPolicy factorized, BEPolicy original){
        long lfact = computeL(factorized);
        long lorg = computeL(original);
        return lfact -lorg;
    }

    private boolean overlaps(ObjectCondition o1, ObjectCondition o2){
        if(o1.getType().getID() == 4){ //Integer
            int start1 = Integer.parseInt(o1.getBooleanPredicates().get(0).getValue());
            int end1 = Integer.parseInt(o1.getBooleanPredicates().get(1).getValue());
            int start2 = Integer.parseInt(o2.getBooleanPredicates().get(0).getValue());
            int end2 = Integer.parseInt(o2.getBooleanPredicates().get(1).getValue());
            if (start1 <= end2 && end1 <= start2)
                return true;
        }
        else if(o1.getType().getID() == 2) { //Timestamp
            Calendar start1 = timestampStrToCal(o1.getBooleanPredicates().get(0).getValue());
            Calendar end1 = timestampStrToCal(o1.getBooleanPredicates().get(1).getValue());
            Calendar start2 = timestampStrToCal(o2.getBooleanPredicates().get(0).getValue());
            Calendar end2 = timestampStrToCal(o2.getBooleanPredicates().get(1).getValue());
            if (start1.compareTo(end2) < 0 && end1.compareTo(start2) < 0) {
                return true;
            }
        }
        else{
            throw new PolicyEngineException("Incompatible Attribute Type");
        }
        return false;
    }


    private Set<ObjectCondition> getPredicates (String attribute){
        Set<ObjectCondition> predSet = new HashSet<>();
        for (int i = 0; i < expression.getPolicies().size(); i++) {
            BEPolicy pol = expression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(i);
                if(oc.getAttribute().equalsIgnoreCase(attribute)){
                    if(!predSet.contains(oc))
                        predSet.add(oc);
                }
            }
        }
        return predSet;
    }


    private HashMap<BEPolicy, ObjectCondition> getPredicatesOnAttr(String attribute){
        HashMap<BEPolicy, ObjectCondition> predMap = new HashMap<>();
        for (int i = 0; i < expression.getPolicies().size(); i++) {
            BEPolicy pol = expression.getPolicies().get(i);
            for (int j = 0; j < pol.getObject_conditions().size(); j++) {
                ObjectCondition oc = pol.getObject_conditions().get(i);
                if(oc.getAttribute().equalsIgnoreCase(attribute)){
                    predMap.put(pol, oc);
                    continue;
                }
            }
        }
        return predMap;
    }

    private void approximateFactorization(){
        List<String> attributes = new ArrayList<>();
        attributes.add("USER_ID");
        attributes.add("LOCATION_ID");
        attributes.add("TIMESTAMP");
        for (int i = 0; i < attributes.size(); i++) {
            HashMap<BEPolicy, ObjectCondition> predOnAttr = getPredicatesOnAttr(attributes.get(i));

        }


    }




}
