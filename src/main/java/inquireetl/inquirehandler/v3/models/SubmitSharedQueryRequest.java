package inquireetl.inquirehandler.v3.models;

import java.util.Date;

public class SubmitSharedQueryRequest extends QueryRequest {
    private String identifier;

    public SubmitSharedQueryRequest() {
    }

    public SubmitSharedQueryRequest(Boolean validateQuery, Integer esPageSize, Date from, Date to, String identifier) {
        super(validateQuery, esPageSize, from, to);
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return this.identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
}

