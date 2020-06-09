package edu.uci.ics.tippers.generation.data.Mall;

import java.util.List;

public class MallShop {

    String shop_name;

    int id;

    String type;

    int capacity;

    List<Integer> wifiaps;


    public MallShop(String shop_name, int id, String type, int capacity, List<Integer> wifiaps) {
        this.shop_name = shop_name;
        this.id = id;
        this.type = type;
        this.capacity = capacity;
        this.wifiaps = wifiaps;
    }
}
