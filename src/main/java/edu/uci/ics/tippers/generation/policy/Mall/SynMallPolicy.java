package edu.uci.ics.tippers.generation.policy.Mall;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.generation.data.Mall.MallDataGeneration;
import edu.uci.ics.tippers.generation.data.Mall.MallShop;
import edu.uci.ics.tippers.persistor.PolicyPersistor;
import edu.uci.ics.tippers.model.policy.BEPolicy;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class SynMallPolicy {

    MPolicyGen mpg;
    MallDataGeneration mdg;
    PolicyPersistor polper;
    Random r;

    List<Integer> devices;
    List<MallShop> mallShops;
    List<String> interests;
    Map<Integer, String> userToInterest;
    Map<Integer, List<String>> userToShop;

    private LocalDate startDate = null, endDate = null;

    private static final String SHOP_OPEN = "08:00";
    private static final double DEFAULT_CHANCE = 0.4;
    private static final double TIMESTAMP_CHANCE = 0.8;
    private static final double LOCATION_CHANCE = 0.5;
    private static final int REGULAR_USER_SHOPS = 6;


    public SynMallPolicy(){
        PolicyConstants.initialize();
        mpg = new MPolicyGen();
        mdg = new MallDataGeneration();
        polper = PolicyPersistor.getInstance();
        r = new Random();

        devices = mpg.retrieveAllDevices();
        interests = mpg.retrieveAllInterests();
        userToShop = mpg.retrieveUserShops();
        userToInterest = mpg.retrieveUserInterest();
        mallShops = mdg.readCoverage();
    }

    public LocalDate getObsDate(String valType){
        if (valType.equalsIgnoreCase("MIN")) {
            if(startDate == null) startDate = mpg.retrieveDate(valType);
            return startDate;
        } else {
            if(endDate == null) endDate = mpg.retrieveDate(valType);
            return endDate;
        }
    }

    private void generateRegularPolicy(int device_id){
        List<BEPolicy> regularPolicies = new ArrayList<>();
        List<String> regularShops = userToShop.get(device_id);
        if(!regularShops.isEmpty()) {
            int shopNum = Math.min(regularShops.size(), REGULAR_USER_SHOPS);
            for (int i = 0; i < shopNum; i++) {
                TimeStampPredicate tsPred = null;
                if (TIMESTAMP_CHANCE > Math.random()) {
                    int duration = r.nextInt(9) + 1;
                    duration = duration * 60;
                    tsPred = new TimeStampPredicate(getObsDate("MIN"), getObsDate("MAX"), SHOP_OPEN, duration);
                }
                String shop_name = regularShops.get(i);
                int querier = Objects.requireNonNull(mallShops.stream().filter(m -> m.getShop_name().equalsIgnoreCase(shop_name)).findAny().orElse(null)).getId();
                regularPolicies.add(mpg.generatePolicies(querier, device_id, regularShops.get(i), tsPred,
                        null, PolicyConstants.ACTION_ALLOW));
            }
        }
        polper.insertPolicy(regularPolicies);
    }

    private void generateIrregularPolicy(int device_id){
        List<BEPolicy> irregularPolicies = new ArrayList<>();
        String userInterest = interests.get(r.nextInt(interests.size()));
        List<MallShop> userPrefShops = mallShops.stream().filter(m -> m.getType().equalsIgnoreCase(userInterest)).collect(Collectors.toList());
        for (MallShop ums: userPrefShops) {
            if(ums.getType().equalsIgnoreCase("restaurant")){
                String LUNCH_START = "11:30";
                int LUNCH_HOURS = 240; //in minutes
                TimeStampPredicate lunchHours = new TimeStampPredicate(getObsDate("MIN"), getObsDate("MAX"),
                        LUNCH_START, LUNCH_HOURS);
                irregularPolicies.add(mpg.generatePolicies(ums.getId(), device_id, ums.getShop_name(), lunchHours,
                        null, PolicyConstants.ACTION_ALLOW));
            }
            else {
                int offset = 0, duration = 0, day = 0;
                TimeStampPredicate tsPred = null;
                if (TIMESTAMP_CHANCE > Math.random()) {
                    offset = r.nextInt(8) + 1;
                    offset = offset * 60; //converting to minutes
                    duration = r.nextInt(3) + 1;
                    duration = duration * 60;
//                    day = r.nextInt(10);
                    tsPred = new TimeStampPredicate(getObsDate("MIN"), day, SHOP_OPEN, offset,  duration);
                }
                irregularPolicies.add(mpg.generatePolicies(ums.getId(), device_id, ums.getShop_name(), tsPred,
                        null, PolicyConstants.ACTION_ALLOW));
            }
        }
        polper.insertPolicy(irregularPolicies);
    }


    private void generateInterestPolicy(int device_id, String user_interest){
        List<BEPolicy> interestPolicies = new ArrayList<>();
        List<MallShop> userPrefShops = mallShops.stream().filter(m -> m.getType().equalsIgnoreCase(user_interest)).collect(Collectors.toList());
        for (MallShop ums: userPrefShops) {
            if(ums.getType().equalsIgnoreCase("restaurant")){
                String LUNCH_START = "11:30";
                int LUNCH_HOURS = 240; //in minutes
                TimeStampPredicate lunchHours = new TimeStampPredicate(getObsDate("MIN"), getObsDate("MAX"),
                        LUNCH_START, LUNCH_HOURS);
                interestPolicies.add(mpg.generatePolicies(ums.getId(), 0, null, lunchHours,
                        user_interest, PolicyConstants.ACTION_ALLOW));
            }
            else {
                int offset = r.nextInt(8) + 1;
                offset = offset * 60; //converting to minutes
                int duration = r.nextInt(3) + 1;
                duration = duration * 60;
                int day = 0;
//              int day = r.nextInt(10);
                TimeStampPredicate tsPred = TIMESTAMP_CHANCE > Math.random()?
                        new TimeStampPredicate(getObsDate("MIN"), day, SHOP_OPEN, offset, duration): null;
                String shopPred = LOCATION_CHANCE > Math.random()? ums.getShop_name(): null;
                if(tsPred == null && shopPred == null)
                    tsPred = new TimeStampPredicate(getObsDate("MIN"), day, SHOP_OPEN, offset, duration);
                interestPolicies.add(mpg.generatePolicies(ums.getId(), 0, shopPred, tsPred,
                        user_interest, PolicyConstants.ACTION_ALLOW));
            }
        }
        polper.insertPolicy(interestPolicies);
    }

    /**
     * get all device_ids
     * if random > 0.5 then regular else irregular
     * for regular, we need to get their most visited shops (user,  shop_name group by user)
     * create a working hours policy with some noise added?
     * if < 0.5 for irregular get a random shop type, create policies for that shop type
     * if shop type is restaurant use lunch hours
     * otherwise use a random period of four hours
     * if the user has interest, create an interest based shop and mall policy
     * write policy to database
     */
    private void generatePolicies(){
        for (int i = 0; i < devices.size(); i++) {
            int dev = devices.get(i);
            if(Math.random() > DEFAULT_CHANCE) generateRegularPolicy(dev);
            else generateIrregularPolicy(dev);
            String user_interest = userToInterest.get(dev);
            if(user_interest != null) generateInterestPolicy(dev, user_interest);
            if (i%100 == 0) System.out.println("Devices completed " + i);
        }
    }

    public static void main(String [] args){
        SynMallPolicy smp = new SynMallPolicy();
        smp.generatePolicies();
    }

}
