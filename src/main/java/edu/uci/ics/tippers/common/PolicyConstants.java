package edu.uci.ics.tippers.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.tippers.model.data.UserProfile;
import edu.uci.ics.tippers.model.policy.Operation;
import edu.uci.ics.tippers.model.policy.QuerierCondition;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Author primpap
 */
public class PolicyConstants {

    public static final String CONJUNCTION = " AND ";

    public static final String DISJUNCTION = " OR ";

    public static final String UNION  = " UNION ";

    public static final String UNION_ALL = " UNION ALL ";

    //EXPERIMENTAL PARAMETERS
    //TODO: Initialize this from the database
    public static final long NUMBER_OR_TUPLES = 3896277;

    public static final long INFINTIY = 10000000000000L;

    public static final int BATCH_SIZE_INSERTION = 50000;

    public static final Duration MAX_DURATION = Duration.ofSeconds(10000000, 0);

    //SERVER COST AND ENGINE COST PARAMETERS DEFAULT VALUES FROM MYSQL
    public static final double IO_BLOCK_READ_COST = 1;

    public static final double MEMORY_BLOCK_READ_COST = 0.25;

    public static final double ROW_EVALUATE_COST = 0.01;

    public static final double KEY_COMPARE_COST = 0.1;

    public static final double UDF_INVOCATION_COST = 0.00054; //includes cost of policy evaluation

    public static final double POLICY_EVAL_COST = 0.0000044;

    public static final double NUMBER_OF_PREDICATES_EVALUATED = 0.66;

    public static final String DATE_FORMAT = "yyyy-MM-dd";

    public static final String TIME_FORMAT = "HH:mm:ss";

    //TIMESTAMP FORMAT
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    //QUERIES FOR EXPERIMENTATION
    public static final String SELECT_ALL_SEMANTIC_OBSERVATIONS = "Select * from PRESENCE  ";

    public static final String SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE = "Select * from PRESENCE WHERE ";

    public static final String ORDER_BY_ID = " order by id ";



    //POLICY GENERATION PARAMETERS
    public static final int LOW_TEMPERATURE = 55;

    public static final int HIGH_TEMPERATURE = 75;

    public static final int LOW_WEMO = 0;

    public static final int HIGH_WEMO = 100;

    public static final String START_TS = "2017-03-31 15:10:00 ";

    public static final String END_TS = "2017-12-07 16:24:57";

    public static final ImmutableList<String> ACTIVITIES = ImmutableList.of("class", "meeting", "seminar",
            "private", "walking", "unknown", "work");

    public static final List<String> USER_PROFILES = Stream.of(UserProfile.values()).map(UserProfile::getValue).collect(Collectors.toList());

    public static final ImmutableList<Double> HOUR_EXTENSIONS = ImmutableList.of(144.0, 168.0, 180.0, 200.0, 300.0, 700.0, 1000.0);

    //TEMPORARY FIX: Querier Conditions
    public static final ImmutableList<QuerierCondition> DEFAULT_QC = ImmutableList.
            of(new QuerierCondition("test", "policy_type",AttributeType.STRING, Operation.EQ,"user"),
                    new QuerierCondition("test", "querier", AttributeType.STRING, Operation.EQ, "10"));

    //ATTRIBUTE NAMES
    public static final ImmutableList<String> ATTR_LIST = ImmutableList.of("user_id", "location_id", "user_profile",
            "user_group", "start_date", "start_time");

    //INDEXED ATTRIBUTES
    public static final ImmutableList<String> INDEX_ATTRS = ImmutableList.of("user_id", "location_id", "user_profile",
            "user_group", "start_date", "start_time");

    //RANGED ATTRIBUTE NAMES
    public static final ImmutableList<String> RANGE_ATTR_LIST = ImmutableList.of("start_date", "start_time");

    public static final String START_DATE = "start_date";

    public static final String START_TIME = "start_time";

    public static final String LOCATIONID_ATTR = "location_id";

    public static final String USERID_ATTR = "user_id";

    public static final String GROUP_ATTR = "user_group";

    public static final String PROFILE_ATTR = "user_profile";

    //Auxiliary attributes

    public static final String ENERGY_ATTR = "energy";

    public static final String TEMPERATURE_ATTR = "temperature";

    public static final String ACTIVITY_ATTR = "activity";


    //DIRECTORY PATHS
    public static final String BE_POLICY_DIR = "results/be_policies/";

    public static final String HISTOGRAM_DIR = "histogram/";

    public static final String QUERY_FILE = "queries.txt";

    public static final String QUERY_RESULTS_DIR = "query_results/"; //results from traditional query rewrite

    public static final String QR_FACTORIZED = "query_results/factorized/"; //results from greedy exact

    public static final String QR_EXTENDED = "query_results/extended/"; //results from approximation/extension

    //TODO: Read this automatically
    //Indices available in the database
    public static final ImmutableMap<String, String> ATTRIBUTE_IND =
            new ImmutableMap.Builder<String, String>()
                    .put(PolicyConstants.USERID_ATTR, "user_hash")
                    .put(PolicyConstants.GROUP_ATTR, "group_hash")
                    .put(PolicyConstants.PROFILE_ATTR, "profile_hash")
                    .put(PolicyConstants.START_TIME, "time_tree")
                    .put(PolicyConstants.START_DATE, "date_tree")
                    .put(PolicyConstants.LOCATIONID_ATTR, "loc_hash")
                    .build();


    public static final String ACTION_ALLOW = "allow";
    public static final String ACTION_DENY = "deny";
    public static final String USER_INDIVIDUAL = "user";
    public static final String USER_GROUP = "group";
}
