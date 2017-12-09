package edu.uci.ics.tippers.common;

/**
 * Author primpap
 */
public class PolicyConstants {

    public static final String CONJUNCTION = " AND ";

    public static final String DISJUNCTION = " OR ";

    public static final long INFINTIY = 10000000000000L;

    public static final String SELECT_ALL_SEMANTIC_OBSERVATIONS = "Select * from SEMANTIC_OBSERVATION";

    public static final String SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS = "Select count(*) from SEMANTIC_OBSERVATION";

    public static final int BATCH_SIZE_INSERTION = 50000;

    public static int LOG_LIMIT = 100000;

}
