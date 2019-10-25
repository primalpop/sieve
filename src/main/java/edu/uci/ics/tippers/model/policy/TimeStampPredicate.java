package edu.uci.ics.tippers.model.policy;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TimeStampPredicate {

    LocalDate startDate;
    LocalDate endDate;
    LocalTime startTime;
    LocalTime endTime;

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");

    public TimeStampPredicate(Timestamp origin, Timestamp termination, String start, int duration) {
        this.startDate = origin.toLocalDateTime().toLocalDate();
        this.endDate = termination.toLocalDateTime().toLocalDate();
        this.startTime = LocalTime.parse(start);
        this.endTime = this.startTime.plus(duration, ChronoUnit.MINUTES);
    }

    public TimeStampPredicate(Timestamp origin, int week, String start, int offset, int duration) {
        this.startDate = origin.toLocalDateTime().toLocalDate();
        this.startDate = this.startDate.plus(week, ChronoUnit.WEEKS);
        this.endDate = this.startDate.plus(1, ChronoUnit.WEEKS);
        this.startTime = LocalTime.parse(start);
        this.startTime = this.getStartTime().plus(offset, ChronoUnit.MINUTES);
        this.endTime = this.startTime.plus(duration, ChronoUnit.MINUTES);
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    public LocalTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalTime startTime) {
        this.startTime = startTime;
    }

    public LocalTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalTime endTime) {
        this.endTime = endTime;
    }

    public String parseStartTime(){
        return this.startTime.format(dtf);
    }

    public String parseEndTime(){
        return this.endTime.format(dtf);
    }
}
