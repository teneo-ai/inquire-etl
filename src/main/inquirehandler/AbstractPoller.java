package main.inquirehandler;

import main.inquirehandler.v1.models.AbstractQueryResultMessage;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Map;

public abstract class AbstractPoller extends InquireRest {

    public Object timeout;

    public AbstractPoller(WebTarget webTarget, AuthorizationFilter authorizationFilter) throws IllegalArgumentException {
        super(webTarget, authorizationFilter);
    }

    public abstract boolean poll() throws Exception;

    public abstract Iterable<Map<String, Object>> getResults();

    public abstract AbstractQueryResultMessage parseResponse(Response pollResponse) throws Exception;
}
