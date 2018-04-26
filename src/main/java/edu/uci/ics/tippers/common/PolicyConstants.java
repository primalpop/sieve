package edu.uci.ics.tippers.common;

import com.google.common.collect.ImmutableList;
import edu.uci.ics.tippers.model.policy.QuerierCondition;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

/**
 * Author primpap
 */
public class PolicyConstants {

    public static final String CONJUNCTION = " AND ";

    public static final String DISJUNCTION = " OR ";

    //EXPERIMENTAL PARAMETERS
    //TODO: Initialize this from the database
    public static final long NUMBER_OR_TUPLES = 1000000;

    public static final long INFINTIY = 10000000000000L;

    public static final int BATCH_SIZE_INSERTION = 50000;

    public static final Duration MAX_DURATION = Duration.ofSeconds(10000000, 0);

    //TIMESTAMP FORMAT
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
                .appendPattern(TIMESTAMP_FORMAT)
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .toFormatter();



    //QUERIES FOR EXPERIMENTATION
    public static final String SELECT_ALL_SEMANTIC_OBSERVATIONS = "Select * from SEMANTIC_OBSERVATION WHERE ";

    public static final String SELECT_COUNT_STAR_SEMANTIC_OBSERVATIONS = "Select count(*) from SEMANTIC_OBSERVATION";


    //POLICY GENERATION PARAMETERS
    public static final int LOW_TEMPERATURE = 55;

    public static final int HIGH_TEMPERATURE = 75;

    public static final int LOW_WEMO = 0;

    public static final int HIGH_WEMO = 100;

    public static final String START_TS = "2017-03-31 15:10:00 ";

    public static final String END_TS = "2017-10-23 12:40:55";

    public static final ImmutableList<String> ACTIVITIES = ImmutableList.of("class", "meeting", "seminar",
            "private", "walking", "unknown", "work");

    public static final ImmutableList<Double> HOUR_EXTENSIONS = ImmutableList.of(1.0, 2.0, 3.0, 4.0,
            5.0, 6.0, 8.0, 10.0, 12.0, 24.0, 48.0, 72.0, 168.0, 336.0);


    //TEMPORARY FIX: Querier Conditions
    public static final ImmutableList<QuerierCondition> DEFAULT_QC = ImmutableList.
            of(new QuerierCondition("user_name",AttributeType.STRING, "=","John Doe", "=", "John Doe"));

    //INDEXED ATTRIBUTES
    public static final ImmutableList<String> INDEXED_ATTRS = ImmutableList.of("timeStamp", "location_id");


    //ATTRIBUTE NAMES

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
}
