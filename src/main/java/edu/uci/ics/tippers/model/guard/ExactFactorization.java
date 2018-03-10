package edu.uci.ics.tippers.model.guard;

import com.github.davidmoten.guavamini.Lists;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExactFactorization {


    Map<BEExpression, ExactFactor> fMap = new HashMap<>();

    public ExactFactorization(){

    }

    public void memoize(BEExpression beExpression){
        List<BEPolicy> policyList = beExpression.getPolicies();
        for (int i = 0; i < policyList.size(); i++) {
            BEExpression policyExp = new BEExpression();
            policyExp.setPolicies(Collections.singletonList(policyList.get(i)));
            ExactFactor ef = new ExactFactor(null, beExpression.computeCost());
            fMap.put(policyExp, ef);
        }

        boolean roll = true;
        List<BEExpression> nextLevel = Lists.newArrayList(fMap.keySet());
        while(roll){
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
