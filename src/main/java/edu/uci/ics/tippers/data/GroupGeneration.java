package edu.uci.ics.tippers.data;

import com.opencsv.CSVReader;
import edu.uci.ics.tippers.model.data.TimePeriod;
import edu.uci.ics.tippers.model.data.UserGroup;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class GroupGeneration {

    private static final String quarterEnd = "2018-10-31 00:00:00";

    private static final String quarterStart = "2018-10-00 00:00:00";

    private static final SimpleDateFormat mSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final AtomicLong counter = new AtomicLong(0);


    private static long getNextNumber() {
        return counter.incrementAndGet();
    }

    private long dayToMiliseconds(int days){
        return (long) (days * 24 * 60 * 60 * 1000);
    }

    private Timestamp addDays(int days, Timestamp t1) throws Exception{
        if(days < 0){
            throw new Exception("Day in wrong format.");
        }
        long miliseconds = dayToMiliseconds(days);
        return new Timestamp(t1.getTime() + miliseconds);
    }

    private List<TimePeriod> getTimeStamps(String day, String timeFrom, String timeTo) throws Exception {
        List<TimePeriod> tps = new ArrayList<>();
        Timestamp endTS = new Timestamp((mSdf.parse(quarterEnd).getTime()));
        TimePeriod tp = new TimePeriod();
        Timestamp fTS = getDate(day + ", " + timeFrom);
        Timestamp tTS = getDate(day + ", " + timeTo);
        tp.setStart(fTS);
        tp.setEnd(tTS);
        tps.add(tp);
        Timestamp cTS = addDays(7, fTS);
        while(cTS.before(endTS)){
            tp = new TimePeriod();
            tp.setStart(cTS);
            tp.setEnd(addDays(7, tTS));
            tps.add(tp);
            cTS = addDays(7, cTS);
            tTS = addDays(7, tTS);
        }
        return tps;
    }


    public List<UserGroup> readCSVFile(String csvFile){
        String line = "";
        String csvSplitBy = ",";

        CSVReader csvReader = new CSVReader(new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(csvFile))));

        List<UserGroup> userGroups = new ArrayList<UserGroup>();

        String[] record = null;

        try{
            while ((record = csvReader.readNext()) != null) {
                UserGroup ug = new UserGroup();
                ug.setGroup_type("class");
                ug.setGroup_id((int) getNextNumber());
                ug.setName(record[0]);
                ug.setLocation(record[6]);
                String timeFrom = record[4];
                String timeTo = record[5];
                List<TimePeriod> tps = new ArrayList<>();
                String day1 = record[1];
                if(day1 != null && !day1.isEmpty())
                    tps.addAll(getTimeStamps(day1, timeFrom, timeTo));
                String day2 = record[2];
                if(day2 != null && !day2.isEmpty())
                    tps.addAll(getTimeStamps(day2, timeFrom, timeTo));
                String day3 = record[3];
                if(day3 != null && !day3.isEmpty())
                    tps.addAll(getTimeStamps(day3, timeFrom, timeTo));
                ug.setTimePeriods(tps);
                userGroups.add(ug);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userGroups;
    }


    /**
     * https://stackoverflow.com/questions/37306199/convert-day-of-week-timehhmm-to-date-in-java
     * @param input
     */
    private Timestamp getDate(String input) {

        long MILLISECONDS_PER_WEEK = 7L * 24 * 60 * 60 * 1000;

        // Parse given date. Convert this to milliseconds since epoch. This will
        // result in a date during the first week of 1970.
        SimpleDateFormat sdf = new SimpleDateFormat("E, H:mm");
        Date date = sdf.parse(input, new ParsePosition(0));

        // Convert to millis and adjust for offset between today's Daylight
        // Saving Time (default for the sdf) and the 1970 date
        Calendar c = Calendar.getInstance();
        int todayDSTOffset = c.get(Calendar.DST_OFFSET);
        c.setTime(date);
        int epochDSTOffset = c.get(Calendar.DST_OFFSET);


        Date startDate = null;
        try {
            startDate = mSdf.parse(quarterStart);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        long parsedMillis = date.getTime() + (epochDSTOffset - todayDSTOffset);

        // Calculate how many weeks ago that was
        long millisInThePast = startDate.getTime() - parsedMillis;
        long weeksInThePast = millisInThePast / MILLISECONDS_PER_WEEK;
        // Add that number of weeks plus 1
        Date output = new Date(parsedMillis + (weeksInThePast + 1) * MILLISECONDS_PER_WEEK);

        return new Timestamp(output.getTime());
    }

}
