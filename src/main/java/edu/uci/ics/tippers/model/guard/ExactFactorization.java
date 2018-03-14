package edu.uci.ics.tippers.model.guard;

import com.github.davidmoten.guavamini.Lists;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class ExactFactorization {


    Map<BEExpression, ExactFactor> fMap = new HashMap<>();

    public ExactFactorization(){

    }

    public void memoize(BEExpression beExpression){
        List<BEPolicy> policyList = beExpression.getPolicies();
        for (int i = 0; i < policyList.size(); i++) {
            List<Factorino> factz = new ArrayList<>();
            BEExpression policyExp = new BEExpression();
            policyExp.setPolicies(Collections.singletonList(policyList.get(i)));
            BEExpression remainder = new BEExpression(beExpression);
            remainder.getPolicies().removeAll(policyExp.getPolicies());
            Set<Set<ObjectCondition>> powerSet = policyList.get(i).calculatePowerSet();
            for (Set<ObjectCondition> objSet: powerSet) {
                if(objSet.size() == 0) continue;
                List<ObjectCondition> multiplier = new ArrayList<>(objSet);
                BEExpression quotient = new BEExpression(policyExp);
                quotient.removeFromPolicies(objSet);
                Factorino factorino = new Factorino(multiplier, quotient);
                factorino.setCost();
                factz.add(factorino);
            }
            ExactFactor ef = new ExactFactor(factz);
            long cost = ef.getMinByCost().getCost() + remainder.computeCost();
            ef.setCost(cost);
            fMap.put(policyExp, ef);
        }
        List<BEExpression> nextLevel = Lists.newArrayList(fMap.keySet());
        while(true){
            for (int i = 0; i < nextLevel.size(); i++) {
                for (int j = 1; j < nextLevel.size(); j++) {

                }
            }
        }
    }

    public Factorino checkCommon(BEExpression b1, BEExpression b2){
        Factorino factorino = new Factorino();
        for (int i = 0; i < b1.getPolicies().size(); i++) {
            for (int j = 0; j < b2.getPolicies().size(); j++){

            }
        }
    }



    public void printfMap(){
        System.out.println("Expression %-8s  Best Factorization %-8s  Cost %-8s Total Cost");
        fMap.forEach((key, value) -> {
            System.out.println(key.createQueryFromPolices() + "%-8s| ");
            if(value.possibleFactz!= null) {
                System.out.print(value.getMinByCost().toString() + "%-8s| " + "| ");
                System.out.print(value.getMinByCost().getCost() + "%-8s| ");
            }
            else
                System.out.println("No factorization possible");
            System.out.println(value.cost);
        });
    }

}
