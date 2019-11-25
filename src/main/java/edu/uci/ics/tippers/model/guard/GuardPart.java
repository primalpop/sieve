package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.model.policy.BEExpression;
import edu.uci.ics.tippers.model.policy.ObjectCondition;

public class GuardPart {

    int id;

    ObjectCondition guard;

    BEExpression guardPartition;

    public ObjectCondition getGuard() {
        return guard;
    }

    public void setGuard(ObjectCondition guard) {
        this.guard = guard;
    }

    public BEExpression getGuardPartition() {
        return guardPartition;
    }

    public void setGuardPartition(BEExpression guardPartition) {
        this.guardPartition = guardPartition;
    }
}
