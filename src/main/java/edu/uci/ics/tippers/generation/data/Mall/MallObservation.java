package edu.uci.ics.tippers.generation.data.Mall;

import java.time.LocalDate;
import java.time.LocalTime;

public class MallObservation {
    String observation_number;

    String shop_name;

    LocalDate obs_date;

    LocalTime obs_time;

    String user_interest;

    int device;

    public MallObservation(String observation_number, String shop_name, LocalDate obs_date, LocalTime obs_time, String user_interest, int device) {
        this.observation_number = observation_number;
        this.shop_name = shop_name;
        this.obs_date = obs_date;
        this.obs_time = obs_time;
        this.device = device;
        this.user_interest = user_interest;
    }

    public LocalDate getObs_date() {
        return obs_date;
    }

    public void setObs_date(LocalDate obs_date) {
        this.obs_date = obs_date;
    }

    public LocalTime getObs_time() {
        return obs_time;
    }

    public void setObs_time(LocalTime obs_time) {
        this.obs_time = obs_time;
    }

    public String getUser_interest() {
        return user_interest;
    }

    public void setUser_interest(String user_interest) {
        this.user_interest = user_interest;
    }


    public String getObservation_number() {
        return observation_number;
    }

    public void setObservation_number(String observation_number) {
        this.observation_number = observation_number;
    }

    public String getShop_name() {
        return shop_name;
    }

    public void setShop_name(String shop_name) {
        this.shop_name = shop_name;
    }

    public int getDevice() {
        return device;
    }

    public void setDevice(int device) {
        this.device = device;
    }
}
