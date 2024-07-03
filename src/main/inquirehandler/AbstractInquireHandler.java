package main.inquirehandler;

import java.net.URL;
import java.util.Map;

public abstract class AbstractInquireHandler extends InquireRest {

    public AbstractInquireHandler(URL serverUrl, String accessToken) throws IllegalArgumentException {
        super(serverUrl, accessToken);
    }

    public abstract String login(final String username, final String password);

    public abstract void logout();

    public abstract AbstractPoller submitSharedQuery(final String ldsName, final String identifier, final Map<String, Object> parameters) throws Exception;

}
