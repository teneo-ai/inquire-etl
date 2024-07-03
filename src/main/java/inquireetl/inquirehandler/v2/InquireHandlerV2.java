package inquireetl.inquirehandler.v2;

import inquireetl.inquirehandler.AbstractInquireHandler;
import inquireetl.inquirehandler.AbstractPoller;
import inquireetl.inquirehandler.v2.models.SharedQuery;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class InquireHandlerV2 extends AbstractInquireHandler {

    public InquireHandlerV2(URL serverUrl, String accessToken) throws IllegalArgumentException {
        super(serverUrl, accessToken);
    }


    @Override
    public String login(String username, String password) {
        // not yet implemented.
        return null;
    }

    @Override
    public void logout() {

    }

    @Override
    public AbstractPoller submitSharedQuery(String ldsName, String identifier, Map<String, Object> parameters) {
        return null;
    }

    public List<SharedQuery> getSharedQueries(String ldsName) {
        return null;
    }
}
