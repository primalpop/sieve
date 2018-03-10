package edu.uci.ics.tippers.model.guard;

import com.github.davidmoten.guavamini.Lists;
import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

import java.util.*;

public class ExactFactor {

    //Stores the different factorization possible
    List<Factorino> possibleFactz;

    //Cost of executing the complete expression: Factorino + Reminder
    long cost;

    public ExactFactor() {
        possibleFactz = new ArrayList<>();
    }

    public void factorize(BEExpression beExpression) {
        Set<Set<ObjectCondition>> powerSet = beExpression.getPolicies().get(0).calculatePowerSet();
        for (int i = 1; i < beExpression.getPolicies().size(); i++) {
            BEPolicy bp = beExpression.getPolicies().get(i);
            powerSet.addAll(bp.calculatePowerSet());
        }
        BEExpression temp = new BEExpression(beExpression);
        for (Set<ObjectCondition> objSet : powerSet) {
            if (objSet.size() == 0) continue;
            temp.checkAgainstPolices(objSet);
            if (temp.getPolicies().size() > 1) {
                Factorino factorino = new Factorino();
                factorino.setMultiplier(Lists.newArrayList(objSet));
                factorino.setQuotient(temp);
                factorino.setCost();
                possibleFactz.add(factorino);
            }
        }
    }

    public List<Factorino> getPossibleFactz() {
        return possibleFactz;
    }

    public void setPossibleFactz(List<Factorino> possibleFactz) {
        this.possibleFactz = possibleFactz;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public Factorino getMinByCost(){
        return this.possibleFactz.stream().min(Comparator.comparing(Factorino::getCost)).orElseThrow(NoSuchElementException::new);
    }
}
