package edu.uci.ics.tippers.generation.query.WiFiDataSet;

import edu.uci.ics.tippers.generation.policy.WiFiDataSet.PolicyUtil;
import edu.uci.ics.tippers.generation.query.QueryGen;
import edu.uci.ics.tippers.model.policy.TimeStampPredicate;
import edu.uci.ics.tippers.model.query.QueryStatement;

import java.sql.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class WiFiDataSetQueryGeneration extends QueryGen {

    private List<Integer> user_ids;
    private List<String> locations;
    private List<String> user_groups;
    Timestamp start_beg, start_fin;
    private List<Integer> hours;
    private List<Integer> numUsers;
    private List<Integer> numLocs;

    private PolicyUtil pg; //helper methods defined in this class

    public WiFiDataSetQueryGeneration() {
        pg = new PolicyUtil();
        this.user_ids = pg.getAllUsers(false);
        this.locations = pg.getAllLocations();
        this.start_beg = pg.getDate("MIN");
        this.start_fin = pg.getDate("MAX");
        this.user_groups = new ArrayList<>();
        user_groups.addAll(locations);
        user_groups.addAll(new ArrayList<>(Arrays.asList("faculty", "staff", "undergrad", "graduate", "visitor")));
        hours = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 7, 10, 12, 15, 17, 20, 23));
//        numUsers = new ArrayList<Integer>(Arrays.asList(10, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450,
//                500, 550, 600, 650, 700, 750, 800, 850, 1000, 1250, 1500, 2000));
        numUsers = new ArrayList<Integer>(Arrays.asList(1000, 2000, 3000, 5000, 10000, 11000, 12000, 13000, 14000, 15000));
        numLocs = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 5, 10, 15, 20, 30, 50));

    }



    /**
     * Query 1: Select * from PRESENCE as P1 where P1.location_id in [......]
     * and start_date >= d1 and start_date <= d2 and
     * start_time >= t1 and start_time <= t2
     * The algorithm iterates from low to high selectivity types and
     * progressively increases the number of location predicates and/or time range
     * to satisfy the required selectivity except when the query is jumping ahead
     * in selectivity (e.g., chosen selType = low, generated query = medium) in which
     * case it waits instead.
     * @param selTypes   - types of query selectivity needed
     * @param queryCount - number of each queries of each selectivity type
     * @return
     */
    @Override
    public List<QueryStatement> createQuery1(List<String> selTypes, int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        int i = 0;
        for (int k = 0; k < selTypes.size(); k++) {
            int numQ = 0;
            int locs = numLocs.get(i);
            int duration = 30; // in minutes
            int days = 0;
            boolean locFlag = true;
            String selType = selTypes.get(k);
            double chosenSel = getSelectivity(selType);
            System.out.println("Chosen Selectivity " + chosenSel);
            do {
                duration = Math.min(duration, 1439); //maximum of a day
                TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), days, "00:00", duration);
                String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                        tsPred.getEndDate().toString());
                query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                        tsPred.getEndTime().toString());
                List<String> locPreds = new ArrayList<>();
                int predCount = 0;
                while (predCount < locs) {
                    locPreds.add(String.valueOf(locations.get(new Random().nextInt(locations.size()))));
                    predCount += 1;
                }
                query += "AND location_id in (";
                query += locPreds.stream().map(item -> "\"" + item + "\", ").collect(Collectors.joining(" "));
                query = query.substring(0, query.length() - 2); //removing the extra comma
                query += ")";
                float selQuery = queryManager.checkSelectivity(query);
                String querySelType = checkSelectivityType(chosenSel, selQuery);
                if(querySelType == null){
                    if(selType.equalsIgnoreCase("low") || selType.equalsIgnoreCase("medium")) {
                        if (duration < 1439) duration += 30;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 1;
                            locFlag = true;
                        }
                    }
                    else{
                        if (duration < 1439) duration += 200;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 5;
                            if(days % 10 == 0) locFlag = true;
                        }
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    System.out.println("Adding query with " + querySelType + " selectivity");
                    queries.add(new QueryStatement(query, 1, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                    chosenSel = getSelectivity(selType);
                    System.out.println("Chosen Selectivity " + chosenSel);
                    continue;
                }
                if (selType.equalsIgnoreCase("low")){
                    if (querySelType.equalsIgnoreCase("medium")
                            || querySelType.equalsIgnoreCase("high")){
                        duration = duration - 15;
                    }
                }
                else if (selType.equalsIgnoreCase("medium")) {
                    if(querySelType.equalsIgnoreCase("low")) {
                        if (duration < 1439) duration += 200;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 1;
                            locFlag = true;
                        }
                    }
                    else if(querySelType.equalsIgnoreCase("high")){
                        duration = duration - 15;
                    }
                }
                else {
                    if (querySelType.equalsIgnoreCase("low")
                            || querySelType.equalsIgnoreCase("medium")){
                        if (duration < 1439) duration += 200;
                        else if (locFlag && ++i < numLocs.size()) {
                            locs = numLocs.get(i);
                            locFlag = false;
                        } else if (!locFlag && days < 90) {
                            days += 2;
                            if(days % 5 == 0) locFlag = true;
                        }
                    }
                }
            } while (numQ < queryCount);
        }
        return queries;
    }

    /**
     * Query 2: Select * from PRESENCE P1 where P1.user_id in [......]
     * and start_date >= d1 and start_date <= d2 and
     * start_time >= t1 and start_time <= t2
     * The algorithm iterates from low to high selectivity types and
     * progressively increases the number of user predicates and/or time range
     * to satisfy the required selectivity except when the query is jumping ahead
     * in selectivity (e.g., chosen selType = low, generated query = medium) in which
     * case it waits instead.
     * @param selTypes   - types of query selectivity needed
     * @param queryCount - number of each queries of each selectivity type
     * @return
     */
     @Override
     public List<QueryStatement> createQuery2(List<String> selTypes, int queryCount) {
        List<QueryStatement> queries = new ArrayList<>();
        int i = 0;
        for (int k = 0; k < selTypes.size(); k++) {
            int numQ = 0;
            int userCount = numUsers.get(i);
            int duration = 300; // in minutes
            int days = 0;
            boolean userFlag = false;
            String selType = selTypes.get(k);
            double chosenSel = getSelectivity(selType);
            System.out.println("Chosen Selectivity " + chosenSel);
            do {
                duration = Math.min(duration, 1439); //maximum of a day
                TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), days, "00:00", duration);
                String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                        tsPred.getEndDate().toString());
                query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                        tsPred.getEndTime().toString());
                List<Integer> userPreds = new ArrayList<>();
                int predCount = 0;
                while (predCount < userCount) {
                    userPreds.add(user_ids.get(new Random().nextInt(user_ids.size())));
                    predCount += 1;
                }
                query += "AND user_id in (";
                query += userPreds.stream().map(item -> "\"" + item + "\", ").collect(Collectors.joining(" "));
                query = query.substring(0, query.length() - 2); //removing the extra comma
                query += ")";
                float selQuery = queryManager.checkSelectivity(query);
                String querySelType = checkSelectivityType(chosenSel, selQuery);
                if(querySelType == null){
                    if(selType.equalsIgnoreCase("low") || selType.equalsIgnoreCase("medium")) {
                        if (duration < 1439) duration += 100;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            userFlag = true;
                        }
                    }
                    else {
                        if (duration < 1439) duration += 500;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 10;
                            userFlag = true;
                        }
                        else
                            userFlag = true;
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    System.out.println("Adding query with " + querySelType + " selectivity");
                    queries.add(new QueryStatement(query, 2, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                    chosenSel = getSelectivity(selType);
                    System.out.println("Chosen Selectivity " + chosenSel);
                    continue;
                }
                if (selType.equalsIgnoreCase("low")){
                    if (querySelType.equalsIgnoreCase("medium")
                            || querySelType.equalsIgnoreCase("high")){
                        duration = duration - 200;
                        if(i> 0)
                            i--;
                    }
                }
                else if (selType.equalsIgnoreCase("medium")) {
                    if(querySelType.equalsIgnoreCase("low")) {
                        if (duration < 1439) duration += 200;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            if(days % 2 == 0) userFlag = true;
                        }
                    }
                    else if(querySelType.equalsIgnoreCase("high")){
                        duration = duration - 200;
                        days -= 2;
                        userCount -= 50;
                    }
                }
                else {
                    if (querySelType.equalsIgnoreCase("low")
                            || querySelType.equalsIgnoreCase("medium")){
                        if (duration < 1439) duration += 200;
                        else if (userFlag && ++i < numUsers.size()) {
                            userCount = numUsers.get(i);
                            userFlag = false;
                        } else if (!userFlag && days < 90) {
                            days += 1;
                            if(days % 2 == 0) userFlag = true;
                        }
                    }
                }
            } while (numQ < queryCount);
        }
        return queries;
    }

    /**
     * Query 3: Select user_id from PRESENCE as p, USER_GROUP_MEMBERSHIP as m
     * where m.id = "x" and p.user_id = m.user_id and
     * p.ts < time and ps.date ();
     * @return
     */
    @Override
    public List<QueryStatement> createQuery3(List<String> selTypes, int queryNum) {
        List<QueryStatement> queries = new ArrayList<>();
        Random r = new Random();
        String user_group = user_groups.get(r.nextInt(user_groups.size()));
        String full_query = String.format("Select PRESENCE.user_id, PRESENCE.location_id, PRESENCE.start_date, " +
                "PRESENCE.start_time, PRESENCE.user_group, PRESENCE.user_profile  " +
                "from PRESENCE, USER_GROUP_MEMBERSHIP " +
                "where USER_GROUP_MEMBERSHIP.user_group_id = \"%s\" AND PRESENCE.user_id = USER_GROUP_MEMBERSHIP.user_id " +
                "AND ", user_group);
        for (int k = 0; k < selTypes.size(); k++) {
            int duration = 50; // in minutes
            int days = 0;
            String selType = selTypes.get(k);
            double chosenSel = getSelectivity(selType);
            System.out.println("Chosen Selectivity " + chosenSel);
            int numQ = 0;
            boolean seedDate = false;
            Timestamp startDate = pg.getDate("MIN");
            do {
                duration = Math.min(duration, 1439); //maximum of a day
                if(!seedDate) {
                    startDate = Timestamp.valueOf(pg.getDate("MIN").toLocalDateTime().toLocalDate().plus(r.nextInt(10), ChronoUnit.DAYS).atStartOfDay());
                    seedDate = true;
                }
                TimeStampPredicate tsPred = new TimeStampPredicate(startDate, days, "00:00", duration);
                String query = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
                        tsPred.getEndDate().toString());
                query += String.format("and start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                        tsPred.getEndTime().toString());
                float selQuery = queryManager.checkSelectivity(query);
                String querySelType = checkSelectivityType(chosenSel, selQuery);
                if(querySelType == null){
                    if(selType.equalsIgnoreCase("low") || selType.equalsIgnoreCase("medium")) {
                        if (duration < 1439) duration += 50;
                        else if (days < 90) days += 1;
                    }
                    else {
                        if (duration < 1439) duration += 100;
                        else if ( days < 90) days += 2;
                    }
                    continue;
                }
                if(selType.equalsIgnoreCase(querySelType)) {
                    System.out.println("Adding query with " + querySelType + " selectivity");
                    queries.add(new QueryStatement(full_query + query, 3, selQuery, querySelType,
                            new Timestamp(System.currentTimeMillis())));
                    numQ++;
                    seedDate = false;
                    chosenSel = getSelectivity(selType);
                    System.out.println("Chosen Selectivity " + chosenSel);
                    continue;
                }
                if (selType.equalsIgnoreCase("low")){
                    if (querySelType.equalsIgnoreCase("medium")
                            || querySelType.equalsIgnoreCase("high")){
                        duration = duration - 100;
                    }
                }
                else if (selType.equalsIgnoreCase("medium")) {
                    if(querySelType.equalsIgnoreCase("low")) {
                        if (duration < 1439) duration += 100;
                        else if (days < 90)
                            days += 1;
                    }
                    else if(querySelType.equalsIgnoreCase("high")){
                        duration = duration - 100;
                        if(days > 1)  days -= 1;
                    }
                }
                else {
                    if (querySelType.equalsIgnoreCase("low")
                            || querySelType.equalsIgnoreCase("medium")){
                        if (duration < 1439) duration += 200;
                        else if (days < 90) {
                            days += 1;
                        }
                    }
                }
            } while (numQ < queryNum);
        }
        return queries;
    }



    /**
     * Query 4: Select location_id, count(*) from PRESENCE
     * where ts-time < time and ts-date < date group by location_id;
     * @return
     */
    @Override
    public List<QueryStatement> createQuery4() {
        List<QueryStatement> queries = new ArrayList<>();
        for (int j =0; j < 200; j++) {
            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), 0, "00:00", 7*j);
            String query = String.format("Select location_id, count(*) from PRESENCE where start_time >= \"%s\" " +
                            "AND start_time <= \"%s\" group by location_id", tsPred.getStartTime().toString(),
                    tsPred.getEndTime().toString());
            String query_for_sel = String.format("start_time >= \"%s\" AND start_time <= \"%s\" ", tsPred.getStartTime().toString(),
                    tsPred.getEndTime().toString());
            float sel = queryManager.checkSelectivity(query_for_sel);
            if (checkStaticRangeSelectivity(sel) == null) continue;
            queries.add(new QueryStatement(query, 4, sel,
                    checkStaticRangeSelectivity(sel), new Timestamp(System.currentTimeMillis())));
            System.out.println("Added query of selectivity " + checkStaticRangeSelectivity(sel));
        }
        int duration =  1439; //maximum of a day
//        for (int i = 0; i < 90; i=i+3) {
//            TimeStampPredicate tsPred = new TimeStampPredicate(pg.getDate("MIN"), i, "00:00", duration);
//            String query = String.format("Select location_id, count(*) from PRESENCE where start_date >= \"%s\" " +
//                            "AND start_date <= \"%s\" group by location_id", tsPred.getStartDate().toString(),
//                    tsPred.getEndDate().toString());
//            String query_for_sel = String.format("start_date >= \"%s\" AND start_date <= \"%s\" ", tsPred.getStartDate().toString(),
//                    tsPred.getEndDate().toString());
//            float sel = mySQLQueryManager.checkSelectivity(query_for_sel);
//            if (checkStaticRangeSelectivity(sel) == null) continue;
//            queries.add(new QueryStatement(query, 4, sel,
//                    checkStaticRangeSelectivity(sel), new Timestamp(System.currentTimeMillis())));
//            System.out.println("Added query of selectivity " + checkStaticRangeSelectivity(sel));
//        }
        return queries;
    }


    public static void main(String[] args) {
        WiFiDataSetQueryGeneration qg = new WiFiDataSetQueryGeneration();
        boolean[] templates = {false, false, true, false};
        int numOfQueries = 1;
        qg.constructWorkload(templates, numOfQueries);
    }

}
