package edu.uci.ics.tippers.model.query;

import java.sql.Timestamp;

public class QueryStatement {

    int id;

    String query;

    int template;

    float selectivity;

    String selectivity_type;

    Timestamp inserted_at;

    public QueryStatement(String query, int template, float selectivity, String selectivity_type, Timestamp inserted_at) {
        this.query = query;
        this.template = template;
        this.selectivity = selectivity;
        this.selectivity_type = selectivity_type;
        this.inserted_at = inserted_at;
    }

    public int getId() {
        return id;
    }

    public int getTemplate() {
        return template;
    }

    public void setTemplate(int template) {
        this.template = template;
    }

    public float getSelectivity() {
        return selectivity;
    }

    public void setSelectivity(float selectivity) {
        this.selectivity = selectivity;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Timestamp getInserted_at() {
        return inserted_at;
    }

    public void setInserted_at(Timestamp inserted_at) {
        this.inserted_at = inserted_at;
    }

    public String getSelectivity_type() {
        return selectivity_type;
    }

    public void setSelectivity_type(String selectivity_type) {
        this.selectivity_type = selectivity_type;
    }
}
