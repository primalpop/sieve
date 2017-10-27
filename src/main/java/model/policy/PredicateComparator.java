package model.policy;

import java.util.Comparator;

/**
 * Created by cygnus on 10/26/17.
 */
public class PredicateComparator implements Comparator<Predicate> {

    @Override
    public int compare(Predicate b1, Predicate b2) {
        if(!b1.getAttribute().equals(b2.getAttribute())) {
            return -1; //Attributes not equal
        }
        if((b1.getOperator()).equals(b2.getOperator())){
            if(b1.getValue().equals(b2.getValue())){
                return 1; //Predicates equal
            }
        }
        return 2; //Predicates not equal

    }
}
