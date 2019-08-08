package edu.uci.ics.tippers.setup.data;

/**
 * Created by cygnus on 12/8/17.
 */
public enum DataFiles {

    INFRA ("infrastructure.json") ,
    GROUP ("group.json"),
    USER ("user.json"),
    SO ("semanticObservation.json"),
    SO_FULL ("semanticObservationFull.json");


    private final String path;

    private DataFiles(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
