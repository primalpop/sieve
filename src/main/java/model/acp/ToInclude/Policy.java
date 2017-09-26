package model.acp.ToInclude;

import model.acp.BooleanCondition;

/**
 * Created by cygnus on 9/25/17.
 */
public abstract class Policy {

    abstract String getID();

    abstract String getDescription();

    abstract String[] getActions();

    abstract BooleanCondition[] getObjectConditions();

    abstract BooleanCondition[] getQuerierConditions();

    abstract String[] getPurposes();

    abstract String[] getAuthors();

    abstract String[] getMetadata();
}
