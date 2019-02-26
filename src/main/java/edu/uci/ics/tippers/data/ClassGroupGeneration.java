package edu.uci.ics.tippers.data;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClassGroupGeneration {

    private List<String> classrooms;

    private List<Double> hourIntervals;

    private List<Integer> numOfDays;

    public ClassGroupGeneration(){
        classrooms = new ArrayList<>(Arrays.asList("1100, 1200, 1300, 1403, 1406, 1407, 1412, 1413, 1420, 1428, 1429, 1431," +
                "1433, 1434, 1500, 1600"));
        hourIntervals = new ArrayList<Double>(Arrays.asList(1.0, 1.5, 2.0, 2.5, 3.0));
        numOfDays = new ArrayList<>(Arrays.asList(1, 2, 3));
    }

    // weighted random number
    // https://stackoverflow.com/questions/28195582/randomly-select-numbers-in-a-range-from-a-specified-probability-distribution
    // https://stackoverflow.com/questions/6737283/weighted-randomness-in-java

    /**
     * for each room
     *  - currentTime = starting at 9 am
     *  - while currenTime < 6 pm
     *  -   pick numOfDays based on weights (if 1 - choose any day; 2 - choose M,W or T,Th or W,F; 3 - M, W, F
     *  -   pick hourInterval based on weights with noise added
     *  -   add it to currentTime
     *  -   give it a name - class1
     *  -   generate the class group
     */



    private class ClassGroup {
        String class_name;
        String location;
        List<String> days;
        String start, end;

//        To get timestamps in required format
//        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm");
//        String time = sdf.format(Calendar.getInstance());

    }
}
