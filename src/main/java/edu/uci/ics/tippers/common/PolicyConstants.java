package edu.uci.ics.tippers.common;

import java.time.Duration;

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

    public static final int LOG_LIMIT = 100000;

    public static final float LOW_TEMPERATURE = 55;

    public static final float HIGH_TEMPERATURE = 75;

    public static final float LOW_WEMO = 0;

    public static final float HIGH_WEMO = 100;

    public static final Duration MAX_DURATION = Duration.ofSeconds(10000000, 0);

    public static final long SHUTDOWN_WAIT = 1000;

    public static final String POLICY_DIR = "policies/";

    public static final long HOUR_IN_MILLISECONDS = 3600000;

}
