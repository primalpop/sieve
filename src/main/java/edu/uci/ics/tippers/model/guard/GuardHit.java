package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GuardHit {

    //Original expression
    Term input;
    Set<ObjectCondition> canFactors;
    List<Term> finalForm;

    public GuardHit(BEExpression originalExp){

        this.input = new Term();
        this.input.setRemainder(originalExp);
        this.input.setQuotient(originalExp);
        finalForm = new ArrayList<>();

        this.canFactors = new HashSet<>();
        Set<ObjectCondition> pFactors = originalExp.getPolicies().stream()
                .flatMap(p -> p.getObject_conditions().stream())
                .filter(o -> PolicyConstants.INDEX_ATTRS.contains(o.getAttribute()))
                .collect(Collectors.toSet());
        for (ObjectCondition pf: pFactors) {
            Boolean match = false;
            for (ObjectCondition cf: canFactors) {
                if(pf.equalsWithoutId(cf)) match = true;
            }
            if(!match) canFactors.add(pf);
        }
    }


    private long benefit(BEExpression quotient){
        return quotient.getPolicies().size();
    }

    private double cost(ObjectCondition factor, BEExpression quotient){
        return quotient.estimateCostOfGuardRep(factor, false);
    }


    private Term generateGuard(Term current){
        Set<ObjectCondition> removal = new HashSet<>();
        double maxUtility = 0.0;
        Term mTerm = new Term();
        for (ObjectCondition objectCondition : this.canFactors) {
            BEExpression tempQuotient = new BEExpression(current.getRemainder());
            tempQuotient.checkAgainstPolices(objectCondition);
            if (tempQuotient.getPolicies().size() > 1) { //was able to factorize
                double utility = benefit(tempQuotient)/cost(objectCondition, tempQuotient);
                if (utility > maxUtility) { //factorized is better than original
                    maxUtility = utility;
                    mTerm.setQuotient(tempQuotient);
                    mTerm.setRemainder(new BEExpression(current.getRemainder()));
                    mTerm.getRemainder().getPolicies().removeAll(mTerm.getQuotient().getPolicies());
                    mTerm.setFactor(objectCondition);
                }
            } else removal.add(objectCondition); //not a factor of at least two policies
        }
        this.canFactors.removeAll(removal);
        return mTerm;
    }


    private void generateAllGuards(Term current) {
        while (true) {
            if (current.getRemainder().getPolicies().size() > 1) {
                if (canFactors.size() > 1) {
                    Term nTerm = generateGuard(current);
                    if (!nTerm.getQuotient().getPolicies().isEmpty()) {
                        System.out.println("Guard generated" + nTerm.getFactor().print() );
                        finalForm.add(nTerm);
                        current = nTerm;
                    } else { //for remainder with no guards
                        for (BEPolicy bePolicy : current.getRemainder().getPolicies()) {
                            double freq = PolicyConstants.NUMBER_OR_TUPLES;
                            ObjectCondition gOC = new ObjectCondition();
                            for (ObjectCondition oc : bePolicy.getObject_conditions()) {
                                if (!PolicyConstants.INDEX_ATTRS.contains(oc.getAttribute())) continue;
                                if (oc.computeL() < freq) {
                                    freq = oc.computeL();
                                    gOC = oc;
                                }
                            }
                            Term rTerm = new Term();
                            rTerm.setFactor(gOC);
                            BEExpression quotient = new BEExpression();
                            quotient.getPolicies().add(bePolicy);
                            rTerm.setQuotient(quotient);
                            finalForm.add(rTerm);
                        }
                        break;
                    }
                } else break;
            } else break;
        }
    }

    public void printAllGuards(){
        generateAllGuards(this.input);
        int count = 0;
        int numOfPolicies = 0;
        for (Term mt: finalForm) {
            count += 1;
            System.out.println(mt.getFactor().print() + PolicyConstants.CONJUNCTION
                    + mt.getQuotient().createQueryFromPolices());
            numOfPolicies += mt.getQuotient().getPolicies().size();
        }
        System.out.println("Number of Guards: " + count + "| Total Policies: " + numOfPolicies);
    }
}
