package edu.uci.ics.tippers.common;

import com.google.common.collect.ImmutableList;
import edu.uci.ics.tippers.db.QueryManager;
import edu.uci.ics.tippers.model.policy.Operation;
import edu.uci.ics.tippers.model.policy.QuerierCondition;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.time.Duration;
import java.util.*;



public class PolicyConstants {

    public static String DBMS_CHOICE;
    public static String DBMS_LOCATION;
    public static String DBMS_CREDENTIALS;
    public static String TABLE_NAME;
    public static String DATE_FORMAT;
    public static String TIME_FORMAT;
    public static String TIMESTAMP_FORMAT;

    public static String SELECT_ALL;
    public static String SELECT_ALL_WHERE;

    //Database related
    public static long INFINTIY ;
    public static int BATCH_SIZE_INSERTION ;
    public static Duration MAX_DURATION;
    public static double IO_BLOCK_READ_COST;
    public static double MEMORY_BLOCK_READ_COST ;
    public static double ROW_EVALUATE_COST ;
    public static double UDF_INVOCATION_COST;
    public static double POLICY_EVAL_COST ;
    public static double NUMBER_OF_PREDICATES_EVALUATED;

    //Dataset related
    public static List<String> ATTRIBUTES;
    public static List<String> INDEXED_ATTRIBUTES;
    public static List<String> RANGED_ATTRIBUTES;
    public static Map<String, String> ATTRIBUTE_INDEXES;

    private static long NUMBER_OF_TUPLES = 0;

    private PolicyConstants(){

    }

    public static void initialize(){

        Configurations configs = new Configurations();
        try {
            Configuration datasetConfig = configs.properties("experiment/dataset.properties");
            DBMS_LOCATION = datasetConfig.getString("location");
            DBMS_CREDENTIALS = datasetConfig.getString("credentials");
            DBMS_CHOICE = datasetConfig.getString("dbms");
            DATE_FORMAT = datasetConfig.getString("date_format");
            TIME_FORMAT = datasetConfig.getString("time_format");
            TIMESTAMP_FORMAT = datasetConfig.getString("timestamp_format");
            TABLE_NAME = datasetConfig.getString("table_name");

            SELECT_ALL = "Select * from " + PolicyConstants.TABLE_NAME + " ";
            SELECT_ALL_WHERE = "Select * from " + PolicyConstants.TABLE_NAME + " where ";

            Configuration dbmsConfig = configs.properties("experiment/" + DBMS_CHOICE + ".properties");
            INFINTIY = dbmsConfig.getLong("infinity");
            BATCH_SIZE_INSERTION = dbmsConfig.getInt("batch_size");
            MAX_DURATION = Duration.ofMillis(dbmsConfig.getLong("timeout"));
            IO_BLOCK_READ_COST =dbmsConfig.getDouble("io_block_read_cost");
            MEMORY_BLOCK_READ_COST = dbmsConfig.getDouble("memory_block_read_cost");
            ROW_EVALUATE_COST = dbmsConfig.getDouble("row_evaluate_cost");
            UDF_INVOCATION_COST = dbmsConfig.getDouble("udf_invocation_cost");
            POLICY_EVAL_COST = dbmsConfig.getDouble("policy_eval_cost");
            NUMBER_OF_PREDICATES_EVALUATED = dbmsConfig.getDouble("number_of_predicates_evaluated");

            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
                    new FileBasedConfigurationBuilder<PropertiesConfiguration>(
                            PropertiesConfiguration.class).configure(params.fileBased()
                            .setListDelimiterHandler(new DefaultListDelimiterHandler(','))
                            .setFile(new File("src/main/resources/experiment/" + TABLE_NAME.toLowerCase() + ".properties")));
            PropertiesConfiguration tableConfig = builder.getConfiguration();

            ATTRIBUTES = tableConfig.getList(String.class, "attrs");
            INDEXED_ATTRIBUTES = tableConfig.getList(String.class, "indexed_attrs");
            RANGED_ATTRIBUTES = tableConfig.getList(String.class, "range_attrs");
            ATTRIBUTE_INDEXES = new HashMap<>();
            for (int i = 0; i < ATTRIBUTES.size(); i++) {
                ATTRIBUTE_INDEXES.put(ATTRIBUTES.get(i), tableConfig.getString(ATTRIBUTES.get(i)));
            }
        }
        catch (ConfigurationException cex) {
            cex.printStackTrace();
        }
    }


    public static long getNumberOfTuples(){
        if(NUMBER_OF_TUPLES == 0){
            QueryManager queryManager  = new QueryManager();
            NUMBER_OF_TUPLES = queryManager.runCountingQuery(null);
        }
        return NUMBER_OF_TUPLES;
    }

    //Simple Constants
    public static final String CONJUNCTION = " AND ";
    public static final String DISJUNCTION = " OR ";
    public static final String UNION  = " UNION ";
    public static final String UNION_ALL = " UNION ALL ";

    public static final String ACTION_ALLOW = "allow";
    public static final String ACTION_DENY = "deny";
    public static final String USER_INDIVIDUAL = "user";
    public static final String USER_GROUP = "group";

    public static final String MYSQL_DBMS = "mysql";
    public static final String PGSQL_DBMS = "postgres";
    public static final String ORDERS_TABLE = "orders";
    public static final String WIFI_TABLE = "presence";

    //DIRECTORY PATHS
    public static final String BE_POLICY_DIR = "results/be_policies/";
    public static final String HISTOGRAM_DIR = "histogram/";

    //Sample Querier Conditions
    public static final ImmutableList<QuerierCondition> DEFAULT_QC = ImmutableList.
            of(new QuerierCondition("test", "policy_type",AttributeType.STRING, Operation.EQ,"user"),
                    new QuerierCondition("test", "querier", AttributeType.STRING, Operation.EQ, "10"));

    //WiFiDataSet attributes
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

    //Orders table attributes
    public static final String ORDER_KEY = "O_ORDERKEY";
    public static final String ORDER_CUSTOMER_KEY = "O_CUSTKEY";
    public static final String ORDER_STATUS = "O_ORDERSTATUS";
    public static final String ORDER_TOTAL_PRICE = "O_TOTALPRICE";
    public static final String ORDER_DATE = "O_ORDERDATE";
    public static final String ORDER_PRIORITY = "O_ORDERPRIORITY";
    public static final String ORDER_CLERK = "O_CLERK";
    public static final String ORDER_PROFILE = "O_PROFILE";

    //Mall observation table attributes
    public static final String M_OBSERVATION_NO = "id";
    public static final String M_WIFI_AP = "wifi_ap";
    public static final String M_DATE = "obs_date";
    public static final String M_TIME = "obs_time";
    public static final String M_DEVICE = "device_id";

}
