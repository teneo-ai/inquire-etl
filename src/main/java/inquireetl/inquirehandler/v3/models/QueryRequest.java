package inquireetl.inquirehandler.v3.models;

import java.util.Date;

public abstract class QueryRequest {
    private Boolean validateQuery;
    private Integer esPageSize;
    private Date from;
    private Date to;

    protected QueryRequest() {
    }

    protected QueryRequest(Boolean validateQuery, Integer esPageSize, Date from, Date to) {
        this.validateQuery = validateQuery;
        this.esPageSize = esPageSize;
        this.from = from;
        this.to = to;
    }

    public Boolean getValidateQuery() {
        return this.validateQuery;
    }

    public void setValidateQuery(Boolean validateQuery) {
        this.validateQuery = validateQuery;
    }

    public Integer getEsPageSize() {
        return this.esPageSize;
    }

    public void setEsPageSize(Integer esPageSize) {
        this.esPageSize = esPageSize;
    }

    public Date getFrom() {
        return this.from;
    }

    public void setFrom(Date from) {
        this.from = from;
    }

    public Date getTo() {
        return this.to;
    }

    public void setTo(Date to) {
        this.to = to;
    }
}
