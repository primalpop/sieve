package edu.uci.ics.tippers.generation.policy.tpch;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DatePredicate {

    LocalDate startDate;
    LocalDate endDate;

    public DatePredicate(LocalDate startDate, LocalDate endDate){
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public DatePredicate(LocalDate startDate, int offset){
        this.startDate = startDate;
        this.endDate = startDate.plus(offset, ChronoUnit.MONTHS);
    }

    public DatePredicate(LocalDate startDate, long offset){
        this.startDate = startDate;
        this.endDate = startDate.plus(offset, ChronoUnit.DAYS);
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
}
