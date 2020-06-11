package edu.uci.ics.tippers.generation.data.Mall;

import java.util.List;
import java.util.Objects;

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


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getShop_name() {
        return shop_name;
    }

    public void setShop_name(String shop_name) {
        this.shop_name = shop_name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MallShop mallShop = (MallShop) o;
        return id == mallShop.id &&
                capacity == mallShop.capacity &&
                shop_name.equals(mallShop.shop_name) &&
                type.equals(mallShop.type) &&
                Objects.equals(wifiaps, mallShop.wifiaps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shop_name, id);
    }
}
