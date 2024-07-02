package main.InquireHandler.v1;

import main.InquireHandler.AbstractInquireHandler;
import main.InquireHandler.AbstractPoller;
import main.InquireHandler.v1.models.*;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InquireHandlerV1 extends AbstractInquireHandler {

    public InquireHandlerV1(final URL serverUrl, final String accessToken) {
        super(serverUrl, accessToken);
    }

    public String login(final String username, final String password) {
        final String accessToken = doPost(webTarget.path("/v1/auth/login"), Entity.json(new Login(username, password)), String.class);
        authorizationFilter.setAccessToken(accessToken);
        return accessToken;
    }

    public void logout() {
        doPost(webTarget.path("/v1/auth/logout"), null);
        authorizationFilter.setAccessToken(null);
    }

    public List<SharedQuery> getSharedQueries(String ldsName){
        return doGet(webTarget.path("/v1/tql/"+ ldsName + "/shared-queries"), new GenericType<>() {
        });
    }

    @Override
    public AbstractPoller submitSharedQuery(final String ldsName, final String identifier, final Map<String, Object> parameters) throws Exception {
        Map<String, Object> queryParameters = new HashMap<>(parameters != null ? parameters : Map.of());
        queryParameters.put("identifier", identifier);

        AbstractQueryResultMessage message =
                submitQueryByTQLStringOrName(webTarget.path("/v1/tql/" + ldsName +"/shared-queries/submit"), queryParameters, null);
        return new QueryPollerV1(webTarget, authorizationFilter, message, queryParameters.get("timeout"));
    }

    private AbstractQueryResultMessage submitQueryByTQLStringOrName(WebTarget webTarget, Map<String, Object> queryParameters, String tqlQuery) throws Exception {
        if (queryParameters != null) {
            for (Map.Entry<String, Object> entry : queryParameters.entrySet()) {
                webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
            }
        }

        Form formData = new Form();
        if (tqlQuery != null) {
            formData = formData.param("query", tqlQuery);
        }
        Response response = doPost(webTarget, Entity.entity(formData, MediaType.APPLICATION_FORM_URLENCODED));

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
