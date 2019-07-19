package edu.uci.ics.tippers.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.tippers.model.policy.QuerierCondition;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public static final long NUMBER_OR_TUPLES = 14675437;

    public static final long INFINTIY = 10000000000000L;

    public static final int BATCH_SIZE_INSERTION = 50000;

    public static final Duration MAX_DURATION = Duration.ofSeconds(10000000, 0);

    //SERVER COST AND ENGINE COST PARAMETERS DEFAULT VALUES FROM MYSQL
    public static final double IO_BLOCK_READ_COST = 1;

    public static final double MEMORY_BLOCK_READ_COST = 0.25;

    public static final double ROW_EVALUATE_COST = 0.01;

    public static final double KEY_COMPARE_COST = 0.1;

    public static final double NUMBER_OF_PREDICATES_EVALUATED = 0.66;

    //TIMESTAMP FORMAT
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
                .appendPattern(TIMESTAMP_FORMAT)
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .toFormatter();



    //QUERIES FOR EXPERIMENTATION
    public static final String SELECT_ALL_SEMANTIC_OBSERVATIONS = "Select SQL_NO_CACHE * from SEMANTIC_OBSERVATION  ";

    public static final String SELECT_ALL_SEMANTIC_OBSERVATIONS_WHERE = "Select SQL_NO_CACHE * from SEMANTIC_OBSERVATION WHERE ";

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

    public static final ImmutableList<Double> HOUR_EXTENSIONS = ImmutableList.of(12.0, 24.0, 48.0, 72.0, 96.0, 120.0, 144.0, 168.0, 180.0, 200.0);



    //TEMPORARY FIX: Querier Conditions
    public static final ImmutableList<QuerierCondition> DEFAULT_QC = ImmutableList.
            of(new QuerierCondition("test", "policy_type",AttributeType.STRING, "=","user"),
                    new QuerierCondition("test", "querier", AttributeType.STRING, "=", "10"));

    //ATTRIBUTE NAMES, ,
    public static final ImmutableList<String> ATTR_LIST = ImmutableList.of("timeStamp", "energy", "temperature",
            "location_id", "activity", "user_id");

    //INDEXED ATTRIBUTES
    public static final ImmutableList<String> INDEX_ATTRS = ImmutableList.of("timeStamp", "energy", "temperature",
            "location_id", "user_id", "activity");

    //RANGED ATTRIBUTE NAMES
    public static final ImmutableList<String> RANGE_ATTR_LIST = ImmutableList.of("timeStamp", "energy", "temperature");


    public static final String TIMESTAMP_ATTR = "timeStamp";

    public static final String LOCATIONID_ATTR = "location_id";

    public static final String USERID_ATTR = "user_id";

    public static final String ENERGY_ATTR = "energy";

    public static final String TEMPERATURE_ATTR = "temperature";

    public static final String ACTIVITY_ATTR = "activity";

    //DIRECTORY PATHS
    public static final String BASIC_POLICY_1_DIR = "results/basic_policies_1/";

    public static final String BASIC_POLICY_2_DIR = "results/basic_policies_2/";

    public static final String RANGE_POLICY_1_DIR = "results/range_policies_1/";

    public static final String RANGE_POLICY_2_DIR = "results/range_policies_2/";

    public static final String BE_POLICY_DIR = "results/be_policies/";

    public static final String HISTOGRAM_DIR = "histogram/";

    public static final String QUERY_FILE = "queries.txt";

    public static final String QUERY_RESULTS_DIR = "query_results/"; //results from traditional query rewrite

    public static final String QR_FACTORIZED = "query_results/factorized/"; //results from greedy exact

    public static final String QR_EXTENDED = "query_results/extended/"; //results from approximation/extension


    //INDICES
    public static final ImmutableMap<String, String> ATTRIBUTE_IND =
            new ImmutableMap.Builder<String, String>()
                    .put(PolicyConstants.USERID_ATTR, "so_user_hash")
                    .put(PolicyConstants.TIMESTAMP_ATTR, "so_ts")
                    .put(PolicyConstants.LOCATIONID_ATTR, "so_l_hash")
                    .put(PolicyConstants.ENERGY_ATTR, "so_e")
                    .put(PolicyConstants.TEMPERATURE_ATTR, "so_t")
                    .put(PolicyConstants.ACTIVITY_ATTR, "so_activity_hash")
                    .build();


    public static final String SELECT_ALL_USE_INDEX = "Select * from SEMANTIC_OBSERVATION USE INDEX (so_ts, so_l, so_e, so_t, so_a) WHERE ";

    public static final String SELECT_ALL_FORCE_INDEX = "Select * from SEMANTIC_OBSERVATION FORCE INDEX (so_u, so_ts, so_l, so_e, so_t, so_a) WHERE ";

}
