package main.inquirehandler.v1.models;

public abstract class AbstractQueryMessage implements Message {

    protected String id;
    protected String lds;
    protected String query;
    protected boolean error = false;
    protected long time;

    protected AbstractQueryMessage() {
    }

    protected AbstractQueryMessage(String id, String lds, String query, long time) {
        this.id = id;
        this.lds = lds;
        this.query = query;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setLds(String lds) {
        this.lds = lds;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public void setTime(long time) {
        this.time = time;
    }

    // this method is needed to make sure Jackson does not complain about the missing setter in deserialization of the type attribute
    public final void setType(final String type) {
        // do nothing
    }

}

