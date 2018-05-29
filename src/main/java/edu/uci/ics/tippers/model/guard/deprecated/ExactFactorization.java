package edu.uci.ics.tippers.model.guard.deprecated;

import com.github.davidmoten.guavamini.Lists;
import edu.uci.ics.tippers.db.MySQLQueryManager;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

/**
 * Incomplete
 *
 * Memoization based implementation of exact factorization
 *
 */

public class ExactFactorization {


    Map<BEExpression, ExactFactor> fMap = new HashMap<>();

    MySQLQueryManager mySQLQueryManager = new MySQLQueryManager();

    public ExactFactorization(){

    }

    public void memoize(BEExpression beExpression){
        List<BEPolicy> policyList = beExpression.getPolicies();
        // Creating the first level in the map by going through the policies and generating
        // elementary factorizations that cen be used in the next level
        for (int i = 0; i < policyList.size(); i++) {
            List<Factorino> factz = new ArrayList<>();
            BEExpression policyExp = new BEExpression();
            policyExp.setPolicies(Collections.singletonList(policyList.get(i)));
            BEExpression remainder = new BEExpression(beExpression);
            remainder.getPolicies().removeAll(policyExp.getPolicies());
            List<ObjectCondition> polObj = policyList.get(i).getObject_conditions();
            for (int j = 0; j < polObj.size(); j++) {
                List<ObjectCondition> multiplier = new ArrayList<>();
                multiplier.add(polObj.get(j));
                BEExpression quotient = new BEExpression(policyExp);
                quotient.removeFromPolicies(polObj.get(j));
                Factorino factorino = new Factorino(multiplier, quotient);
                factorino.setCost();
                factz.add(factorino);
            }
            ExactFactor ef = new ExactFactor(factz);
            long cost = ef.getMinByCost().getCost() + computeCost(remainder);
            ef.setCost(cost);
            fMap.put(policyExp, ef);
        }

        List<BEExpression> currentLevel = Lists.newArrayList(fMap.keySet());
        while(true){
            List<BEExpression> nextLevel = new ArrayList<>();
            for (int i = 0; i < currentLevel.size(); i++) {
                for (int j = 1; j < currentLevel.size(); j++) {


                }
            }
        }
    }

    public List<BEExpression> checkCommon(BEExpression b1, BEExpression b2, int level){
        List<Factorino> b1F = fMap.get(b1).getPossibleFactz();
        List<Factorino> b2F = fMap.get(b2).getPossibleFactz();
        List<BEExpression> cList = new ArrayList<>();
        ExactFactor cEf = new ExactFactor();
        for (int i = 0; i < b1F.size(); i++) {
            for (int j = 0; j < b2F.size(); j++){
                BEExpression cExp = new BEExpression();
                Factorino cFactorino = new Factorino();
                if(b1F.get(i).sameMultiplier(b2F.get(j))){
                    if (level == 1){
                        cExp.getPolicies().addAll(b1.getPolicies());
                        cExp.getPolicies().addAll(b2.getPolicies());
                        cFactorino.setMultiplier(b1F.get(i).getMultiplier());
                        cFactorino.getQuotient().getPolicies().addAll(b1F.get(i).getQuotient().getPolicies());
                        cFactorino.getQuotient().getPolicies().addAll(b2F.get(j).getQuotient().getPolicies());
                        cList.add(cExp);
                        cEf.getPossibleFactz().add(cFactorino);
                    }
                }
            }
        }
        if(cList.size() > 1){ //More than one factorization was possible and can be combined. e.g., ABC + ACD. Assumption: there are no duplicates
            Factorino mFactorino = new Factorino();
            for (int i = 0; i < cEf.getPossibleFactz().size(); i++) {
                mFactorino.getMultiplier().addAll(cEf.getPossibleFactz().get(i).getMultiplier());
            }

        }
        //Incomplete algorithm; temporary fix
        return null;
    }


    public long computeCost(BEExpression beExpression){
        return mySQLQueryManager.runTimedQuery(beExpression.createQueryFromPolices()).toMillis();
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
