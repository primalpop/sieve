package edu.uci.ics.tippers.model.data;

/**
 * Created by cygnus on 7/7/17.
 */
public class Infrastructure {
    //Semantic entity id of the room
    int location_id;
    //Name as per DBH, e.g. 2065. This is used in SemanticObservation table
    String name;
    //Available types: lab, faculty_office, corridor, restroom, utility, kitchen, Test?, Mail room, Conference room, Floor
    int type;

    public Infrastructure(int location_id, String name, int type) {
        this.location_id = location_id;
        this.name = name;
        this.type = type;
    }


    public Infrastructure() {
    }

    public int getLocation_id() {
        return location_id;
    }

    public void setLocation_id(int location_id) {
        this.location_id = location_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
