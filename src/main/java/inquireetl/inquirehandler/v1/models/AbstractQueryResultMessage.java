package inquireetl.inquirehandler.v1.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

public abstract class AbstractQueryResultMessage extends AbstractQueryMessage {

    // note that this annotation is being used as the nulls are important in tql results, so with this annotation they are forced
    // to be returned through the rest api
    @JsonInclude(JsonInclude.Include.ALWAYS)
    protected Iterable<Map<String, Object>> result;

    public AbstractQueryResultMessage() {
        super();
    }

    public AbstractQueryResultMessage(String id, String lds, String query, long time, Iterable<Map<String, Object>> result) {
        super(id, lds, query, time);
        this.result = result;
    }

    public Iterable<Map<String, Object>> getResult() {
        return result;
    }

}

