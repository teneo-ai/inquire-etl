package inquireetl.inquirehandler.v3;

import inquireetl.inquirehandler.AbstractInquireHandler;
import inquireetl.inquirehandler.AbstractPoller;
import inquireetl.inquirehandler.AbstractQueryResultMessage;
import inquireetl.inquirehandler.v3.models.*;
import java.util.Date;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InquireHandlerV3 extends AbstractInquireHandler {

    public InquireHandlerV3(final URL serverUrl, final String accessToken) {
        super(serverUrl, accessToken);
    }

    public String login(final String username, final String password) {
        final String accessToken = doPost(webTarget.path("/v3/auth/login"), Entity.json(new Login(username, password)), String.class);
        authorizationFilter.setAccessToken(accessToken);
        return accessToken;
    }

    public void logout() {
        doPost(webTarget.path("/v3/auth/logout"), null);
        authorizationFilter.setAccessToken(null);
    }

    public List<SharedQuery> getSharedQueries(String ldsName){
        return doGet(webTarget.path("/v3/tql/shared-queries/" + ldsName), new GenericType<>() {
        });
    }

    @Override
    public AbstractPoller submitSharedQuery(final String ldsName, final String identifier, final Map<String, Object> parameters) throws Exception {
        Map<String, Object> queryParameters = new HashMap<>(parameters != null ? parameters : Map.of());
        queryParameters.put("identifier", identifier);
        SubmitSharedQueryRequest sharedQueryRequest = new SubmitSharedQueryRequest();
        sharedQueryRequest.setIdentifier(identifier);
        String timeout = null;
        if (parameters != null) {
            if (parameters.containsKey("from")) {
                long from = Long.parseLong((String) parameters.get("from"));
                sharedQueryRequest.setFrom(new Date(from));
            }
            if (parameters.containsKey("to")) {
                long to = Long.parseLong((String) parameters.get("to"));
                sharedQueryRequest.setFrom(new Date(to));
            }
            if (parameters.containsKey("esPageSize")) {
                Integer esPageSize = Integer.valueOf((String) parameters.get("timeout"));
                sharedQueryRequest.setEsPageSize(esPageSize);
            }
            if (parameters.containsKey("timeout")) {
                timeout = (String) parameters.get("timeout");
            }
        }
        AbstractQueryResultMessage message = submitSharedQueryRequest(webTarget.path("/v3/tql/shared-queries/submit/" + ldsName), sharedQueryRequest, timeout);
        return new QueryPollerV3(webTarget, authorizationFilter, message, queryParameters.get("timeout"));
    }

    private AbstractQueryResultMessage submitSharedQueryRequest(WebTarget webTarget, SubmitSharedQueryRequest request, String timeout) throws Exception {
        if (timeout != null) {
            webTarget = webTarget.queryParam("timeout", timeout);
        }

        Response response = doPost(webTarget, Entity.json(request));

        return parseResponse(response);
    }

    public static AbstractQueryResultMessage parseResponse(Response response) throws Exception {

        if (response.getStatus() == Response.Status.NO_CONTENT.getStatusCode()) {
            throw new Exception("User does not have sufficient rights to perform this operation");
        }

        final Message message = response.readEntity(Message.class);

        if (message instanceof FailureMessage) {
            throw new Exception(((FailureMessage) message).getErrorMessage());
        }

        return (AbstractQueryResultMessage) message;
    }
}
