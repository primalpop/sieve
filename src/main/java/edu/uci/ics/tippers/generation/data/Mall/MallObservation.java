package edu.uci.ics.tippers.generation.data.Mall;

import java.time.LocalDate;
import java.time.LocalTime;

public class MallObservation {
    String observation_number;

    int wifi_ap;

    LocalDate obs_date;

    LocalTime obs_time;

    int shop_type;

    int device;

    public MallObservation(String observation_number, int wifi_ap, LocalDate obs_date, LocalTime obs_time, int device) {
        this.observation_number = observation_number;
        this.wifi_ap = wifi_ap;
        this.obs_date = obs_date;
        this.obs_time = obs_time;
        this.shop_type = shop_type;
        this.device = device;
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

    public Integer getShop_type() {
        return shop_type;
    }

    public void setShop_type(Integer shop_type) {
        this.shop_type = shop_type;
    }


    public String getObservation_number() {
        return observation_number;
    }

    public void setObservation_number(String observation_number) {
        this.observation_number = observation_number;
    }

    public int getWifi_ap() {
        return wifi_ap;
    }

    public void setWifi_ap(int wifi_ap) {
        this.wifi_ap = wifi_ap;
    }

    public int getDevice() {
        return device;
    }

    public void setDevice(int device) {
        this.device = device;
    }
}
