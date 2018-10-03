package edu.uci.ics.tippers.model.data;

/**
 * Created by cygnus on 7/7/17.
 */
public class Infrastructure {
    //Name as per DBH, e.g. 2065. This is used in SemanticObservation table
    String name;
    //Available types: lab, faculty_office, corridor, restroom, utility, kitchen, Test?, Mail room, Conference room, Floor
    String type;
    //Floors: 1,2,3,4,5,6
    int floor;
    //Regions: ISG, Statistics, ML etc
    String region_name;



    public Infrastructure() {
    }

    public Infrastructure(String name, String type, int floor, String region_name) {
        this.name = name;
        this.type = type;
        this.floor = floor;
        this.region_name = region_name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getFloor() {
        return floor;
    }

    public void setFloor(int floor) {
        this.floor = floor;
    }

    public String getRegion_name() {
        return region_name;
    }

    public void setRegion_name(String region_name) {
        this.region_name = region_name;
    }
}
