package edu.uci.ics.tippers.model.guard;

import edu.uci.ics.tippers.common.PolicyConstants;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

public class GuardExp {

    String id;

    String querier_type;

    String querier;

    String purpose;

    String dirty;

    String action;

    Timestamp last_updated;

    List<GuardPart> guardParts;

    public GuardExp(String id, String purpose, String action, Timestamp last_updated, List<GuardPart> guardParts) {
        this.id = id;
        this.purpose = purpose;
        this.action = action;
        this.last_updated = last_updated;
        this.guardParts = guardParts;
    }

    public GuardExp() {
        this.id = UUID.randomUUID().toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQuerier_type() {
        return querier_type;
    }

    public void setQuerier_type(String querier_type) {
        this.querier_type = querier_type;
    }

    public String getQuerier() {
        return querier;
    }

    public void setQuerier(String querier) {
        this.querier = querier;
    }

    public String getPurpose() {
        return purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getDirty() {
        return dirty;
    }

    public void setDirty(String dirty) {
        this.dirty = dirty;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Timestamp getLast_updated() {
        return last_updated;
    }

    public void setLast_updated(Timestamp last_updated) {
        this.last_updated = last_updated;
    }

    public List<GuardPart> getGuardParts() {
        return guardParts;
    }

    public void setGuardParts(List<GuardPart> guardParts) {
        this.guardParts = guardParts;
    }

    public boolean isUserGuard() {
        return this.querier_type.equalsIgnoreCase("user");
    }

    /**
     * Creates the complete guarded query string
     * @return
     */
    public String createQuery(){
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        for (GuardPart gp: this.guardParts) {
            queryExp.append(delim);
            queryExp.append(gp.getGuard().print());
            queryExp.append(PolicyConstants.CONJUNCTION);
            queryExp.append(gp.getGuardPartition().createQueryFromPolices());
            delim = PolicyConstants.DISJUNCTION;
        }
        return queryExp.toString();
    }

    /**
     * (Select * from Presence where G1
     * UNION Select * from Presence where G2
     * ....
     * Select * from Presence where GN)
     * @return
     */
    public String createGuardOnlyQuery(){
        StringBuilder queryExp = new StringBuilder();
        String delim = "";
        for (GuardPart gp: this.guardParts) {
            queryExp.append(delim);
            queryExp.append(PolicyConstants.SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE)
                    .append(gp.getGuard().print());
            delim = PolicyConstants.UNION;
        }
        return  queryExp.toString();
    }
}
