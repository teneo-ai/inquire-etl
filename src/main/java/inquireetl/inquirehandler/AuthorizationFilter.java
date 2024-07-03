package inquireetl.inquirehandler;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;

/**
 * Adds authorization header if {@link AuthorizationFilter#accessToken} is initialized.
 */
public class AuthorizationFilter implements ClientRequestFilter {

    private String accessToken;

    public AuthorizationFilter(final String accessToken) {
        this.accessToken = accessToken;
    }

    public synchronized void setAccessToken(final String accessToken) {
        this.accessToken = accessToken;
    }

    public synchronized String getAccessToken() {
        return this.accessToken;
    }

    @Override
    public void filter(final ClientRequestContext clientRequestContext) {
            clientRequestContext.getHeaders().add(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
    }

}
