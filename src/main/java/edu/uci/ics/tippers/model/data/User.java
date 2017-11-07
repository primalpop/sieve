package edu.uci.ics.tippers.model.data;

/**
 * Created by cygnus on 7/7/17.
 */
public class User {

    int user_id;

    String name;

    String group_name;

    public static int [] users =  {186, 187, 190, 192, 195, 53371,47435,2564,2691,150176,44904,22605,
            53596,990,27262,36981,53033,574,150172,68692,54827,1252,2593,61901,27705,
            53150,1134,150777,54876,63606,152008,138708,150146,3414,150771,6183,1583,
            355,13467,53748,52347,52934,150729,49352,53590,4958,66520,152747,4757,
            58395,150376,150986};


    public static int [] all_users = {};

    public User(int user_id, String name, String group_name) {
        this.user_id = user_id;
        this.name = name;
        this.group_name = group_name;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }
}
