package edu.uci.ics.tippers.model.data;

/**
 * Created by cygnus on 7/7/17.
 */
public class Infrastructure {
    //Semantic entity id of the room
    int location_id;
    //Name as per DBH, e.g. 2065. This is used in Semantic_Observation table
    String name;
    //Available types: lab, faculty_office, corridor, restroom, utility, kitchen, Test?, Mail room, Conference room, Floor
    int type;

    public static int[] locations = {2028,5002,5004,5039,5032,5038,5042,5044,5048,1412,2211,2013,
            1300,3039,2231,2209,1200,2029,2069,2202,2204,2206,2208,1420,2221,2019,
            2051,2058,2059,2008,2061,1423,2089,2099,2065,2002,2004,5211,1427,5069,
            1433,1422,1429,4209,5013,4219,1431,1425,5089,5099,5065,5082,5011,5231,
            5209,5221,6029,2228,2232,2234,2241,2243,3051,4051,2054,2056,2062,2064,
            6011,2212,2214,2216,2219,2222,2224,2226,6049,3019,5051,5008,5058,5059,
            2088,2091,2092,5019,5026,5028,5061,4019,5228,5232,5234,5241,5243,3099,
            2076,2081,2082};

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
