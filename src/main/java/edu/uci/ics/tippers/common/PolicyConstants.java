package edu.uci.ics.tippers.common;

import com.google.common.collect.ImmutableList;
import edu.uci.ics.tippers.model.policy.QuerierCondition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Author primpap
 */
public class PolicyConstants {
    //TODO: Initialize this from the database
    public static final long NUMBER_OR_TUPLES = 100000;

    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

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

    public static final long HOUR_IN_MILLISECONDS = 3600000;

    public static final String BASIC_POLICY_1_DIR = "results/basic_policies_1/";

    public static final String BASIC_POLICY_2_DIR = "results/basic_policies_2/";

    public static final String RANGE_POLICY_1_DIR = "results/range_policies_1/";

    public static final String RANGE_POLICY_2_DIR = "results/range_policies_2/";

    public static final String BE_POLICY_DIR = "results/be_policies/";

    public static final ImmutableList<QuerierCondition> DEFAULT_QC = ImmutableList.
            of(new QuerierCondition("user_name",AttributeType.STRING, "=","John Doe", "=", "John Doe"));


    public static final String TIMESTAMP_ATTR = "timeStamp";

    public static final String LOCATIONID_ATTR = "location_id";

    public static final String USERID_ATTR = "user_id";

    public static final String ENERGY_ATTR = "energy";

    public static final String TEMPERATURE_ATTR = "temperature";

    public static final String ACTIVITY_ATTR = "activity";


}
