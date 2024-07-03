package main.inquirehandler.v1.models;

import java.util.UUID;

public class SharedQuery {

    private UUID id;
    private String publishedName;
    private String description;
    private String query;
    private UUID ldsId;

    public SharedQuery() {
        // default constructor is needed
    }

    public String getPublishedName() {
        return publishedName;
    }

    public void setPublishedName(String publishedName) {
        this.publishedName = publishedName;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setLdsId(UUID ldsId) {
        this.ldsId = ldsId;
    }
}
